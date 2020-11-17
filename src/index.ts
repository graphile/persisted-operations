import { readFileSync, promises as fsp } from "fs";
import { PostGraphileOptions, PostGraphilePlugin } from "postgraphile";

/**
 * Given a persisted operation hash, return the associated GraphQL operation
 * document.
 */
export type PersistedOperationGetter = (hash: string) => string;

// Need to extend PostGraphileOptions types to support this
declare module "postgraphile" {
  interface PostGraphileOptions {
    /**
     * This function will be passed a GraphQL request object (normally `{query:
     * string, variables?: any, operationName?: string, extensions?: any}`, but
     * in the case of persisted operations it likely won't have a `query`
     * property), and must extract the hash to use to identify the persisted
     * operation. For Apollo Client, this might be something like:
     * `request?.extensions?.persistedQuery?.sha256Hash`; for Relay something
     * like: `request?.documentId`.
     */
    hashFromPayload?(request: any): string;

    /**
     * We can read persisted operations from a folder (they must be named
     * `<hash>.graphql`); this is mostly used for PostGraphile CLI. When used
     * in this way, the first request for a hash will read the file
     * synchronously, and then the result will be cached such that the
     * **synchronous filesystem read** will only impact the first use of that
     * hash. We periodically scan the folder for new files, requests for hashes
     * that were not present in our last scan of the folder will be rejected to
     * mitigate denial of service attacks asking for non-existent hashes.
     */
    persistedOperationsDirectory?: string;

    /**
     * An optional string-string key-value object defining the persisted
     * operations, where the keys are the hashes, and the values are the
     * operation document strings to use.
     */
    persistedOperations?: { [hash: string]: string };

    /**
     * If your known persisted operations may change over time, or you'd rather
     * load them on demand, you may supply this function. Note this function is
     * both **synchronous** and **performance critical** so you should use
     * caching to improve performance of any follow-up requests for the same
     * hash. This function is not suitable for fetching operations from remote
     * stores (e.g. S3).
     */
    persistedOperationsGetter?: PersistedOperationGetter;
  }
}

/**
 * This fallback hashFromPayload method is compatible with Apollo Client and
 * Relay.
 */
function defaultHashFromPayload(request: any) {
  return (
    // https://github.com/apollographql/apollo-link-persisted-queries#protocol
    request?.extensions?.persistedQuery?.sha256Hash ||
    // https://relay.dev/docs/en/persisted-queries#network-layer-changes
    request?.documentId
  );
}

/**
 * Given a cache object, returns a persisted operation getter that looks up the
 * given hash in said cache object.
 */
function persistedOperationGetterForCache(cache: { [key: string]: string }) {
  return (key: string) => cache[key];
}

function makeGetterForDirectory(directory: string) {
  // NOTE: it's generally a bad practice to do synchronous filesystem
  // operations in Node servers; however PostGraphile's hooks are synchronous
  // and we want to only load the files on demand, so we have to bite the
  // bullet. To mitigate the impact of this we cache the results, and we
  // periodically scan the folder to see what files it contains so that we
  // can reject requests to non-existent files to avoid DOS attacks having us
  // make synchronous requests to the filesystem.

  let files: string[] = [];

  /**
   * This function must never reject.
   */
  async function scanDirectory() {
    try {
      files = (await fsp.readdir(directory)).filter((name) =>
        name.endsWith(".graphql")
      );
    } catch (e) {
      console.error(`Error occurred whilst scanning '${directory}'`);
      console.error(e);
    } finally {
      // We don't know how long the scanning takes, so rather than setting an
      // interval, we wait 5 seconds between scans before kicking off the next
      // one.
      setTimeout(scanDirectory, 5000);
    }
  }

  scanDirectory();

  const operationFromHash = new Map();
  function getOperationFromHash(hash: string): string {
    if (!/^[a-zA-Z0-9_-]+$/.test(hash)) {
      throw new Error("Invalid hash");
    }
    let operation = operationFromHash.get(hash);
    if (!operation) {
      const filename = `${hash}.graphql`;
      if (!files.includes(filename)) {
        throw new Error(`Could not find file for hash '${hash}'`);
      }
      operation = readFileSync(`${directory}/${filename}`, "utf8");
      operationFromHash.set(hash, operation);
    }
    return operation;
  }

  return getOperationFromHash;
}

const directoryGetterByDirectory = new Map();

/**
 * Given a directory, get or make the persisted operations getter.
 */
function getterForDirectory(directory: string) {
  let getter = directoryGetterByDirectory.get(directory);
  if (!getter) {
    getter = makeGetterForDirectory(directory);
    directoryGetterByDirectory.set(directory, getter);
  }
  return getter;
}

/**
 * Extracts or creates a persisted operation getter function from the
 * PostGraphile options.
 */
function getterFromOptionsCore(options: PostGraphileOptions) {
  const optionsSpecified = Object.keys(options).filter((key) =>
    [
      "persistedOperationsGetter",
      "persistedOperationsDirectory",
      "persistedOperations",
    ].includes(key)
  );
  if (optionsSpecified.length > 1) {
    // If you'd like support for more than one of these options; send a PR!
    throw new Error(
      `'${optionsSpecified.join(
        "' and '"
      )}' were specified, at most one of these operations can be specified.`
    );
  }
  if (options.persistedOperationsGetter) {
    return options.persistedOperationsGetter;
  } else if (options.persistedOperations) {
    return persistedOperationGetterForCache(options.persistedOperations);
  } else if (options.persistedOperationsDirectory) {
    return getterForDirectory(options.persistedOperationsDirectory);
  } else {
    throw new Error(
      "Server misconfiguration issue: persisted operations (operation allowlist) is in place, but the server has not been told how to fetch the allowed operations. Please provide one of the persisted operations configuration options."
    );
  }
}

// TODO: use an LRU? For users using lots of new options objects this will
// cause a memory leak. But LRUs have a performance cost... Maybe switch to LRU
// once the size has grown?
const getterFromOptionsCache = new Map();

/**
 * Returns a cached getter for performance reasons.
 */
function getterFromOptions(options: PostGraphileOptions) {
  let getter = getterFromOptionsCache.get(options);
  if (!getter) {
    getter = getterFromOptionsCore(options);
    getterFromOptionsCache.set(options, getter);
  }
  return getter;
}

/**
 * Given a payload, this method returns the GraphQL operation document
 * (string), or null on failure. It **never throws**.
 */
function persistedOperationFromPayload(
  payload: any,
  options: PostGraphileOptions
): string | null {
  try {
    const hashFromPayload = options.hashFromPayload || defaultHashFromPayload;
    const hash = hashFromPayload(payload);
    if (typeof hash !== "string") {
      throw new Error(
        "We could not find a persisted operation hash string in the request."
      );
    }
    const getter = getterFromOptions(options);
    return getter(hash);
  } catch (e) {
    console.error("Failed to get persisted operation from payload", payload, e);

    // We must not throw, instead just overwrite the query with null (the error
    // will be thrown elsewhere).
    return null;
  }
}

const PersistedQueriesPlugin: PostGraphilePlugin = {
  ["cli:flags:add:webserver"](addFlag) {
    // Add CLI flag. We're adding our plugin name in square brackets to help
    // the user know where the options are coming from.
    addFlag(
      "--persisted-operations-directory <fullpath>",
      "[@graphile/persisted-operations] The path to the directory in which we'd find the persisted query files (each named <hash>.graphql)"
    );

    // The ouput from one plugin is fed as the input into the next, so we must
    // remember to return the input.
    return addFlag;
  },

  ["cli:library:options"](options, { config, cliOptions }) {
    // Take the CLI options and add them as PostGraphile options.
    const { persistedOperationsDirectory = undefined } = {
      ...config["options"],
      ...cliOptions,
    };
    return {
      ...options,
      persistedOperationsDirectory,
    };
  },

  "postgraphile:options"(options) {
    // In case there's a filesystem getter, this lets us get a head-start on
    // scanning the directory before the first request comes in.
    getterFromOptions(options);

    return options;
  },

  // For regular HTTP requests
  "postgraphile:httpParamsList"(paramsList, { options }) {
    return paramsList.map((params: any) => {
      // ALWAYS OVERWRITE, even if invalid; the error will be thrown elsewhere.
      params.query = persistedOperationFromPayload(params, options) as string;
      return params;
    });
  },

  // For websocket requests
  "postgraphile:ws:onOperation"(params, { message, options }) {
    // ALWAYS OVERWRITE, even if invalid; the error will be thrown elsewhere.
    params.query = persistedOperationFromPayload(
      message.payload,
      options
    ) as string;
    return params;
  },
};

module.exports = PersistedQueriesPlugin;
