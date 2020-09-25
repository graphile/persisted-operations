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
     * `request?.extensions?.persistedQuery?.sha256Hash`
     */
    hashFromPayload?(request: any): string;

    /**
     * An optional string-string key-value object defining the persisted
     * operations, where the keys are the hashes, and the values are the
     * operation document strings to use.
     */
    persistedOperations?: { [hash: string]: string };

    /**
     * If your known persisted queries may change over time, or you'd rather
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
 * This fallback hashFromPayload method is compatible with Apollo Client.
 */
function defaultHashFromPayload(request: any) {
  return request?.extensions?.persistedQuery?.sha256Hash;
}

/**
 * Given a cache object, returns a persisted operation getter that looks up the
 * given hash in said cache object.
 */
function persistedOperationGetterForCache(cache: { [key: string]: string }) {
  return (key: string) => cache[key];
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
    const getter =
      options.persistedOperationsGetter ||
      persistedOperationGetterForCache(options.persistedOperations || {});
    const hash = hashFromPayload(payload);
    if (typeof hash !== "string") {
      throw new Error("Invalid operation hash");
    }
    return getter(hash);
  } catch (e) {
    console.error(
      "Failed to get persisted operation from extensions",
      payload,
      e
    );

    // We must not throw, instead just overwrite the query with null (the error
    // will be thrown elsewhere).
    return null;
  }
}

const PersistedQueriesPlugin: PostGraphilePlugin = {
  // For regular HTTP requests
  "postgraphile:httpParamsList"(paramsList, { options }) {
    return paramsList.map((params: any) => {
      // ALWAYS OVERWRITE, even if invalid; the error will be thrown elsewhere.
      params.query = persistedOperationFromPayload(
        params.extensions,
        options
      ) as string;
      return params;
    });
  },

  // For websocket requests
  "postgraphile:ws:onOperation"(params, { message, options }) {
    // ALWAYS OVERWRITE, even if invalid; the error will be thrown elsewhere.
    params.query = persistedOperationFromPayload(
      message.payload.extensions,
      options
    ) as string;
    return params;
  },
};

module.exports = PersistedQueriesPlugin;
