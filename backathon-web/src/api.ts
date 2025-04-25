import createClient, { type FetchResponse, type MaybeOptionalInit } from "openapi-fetch";

import type { paths } from "./schema.d.ts";
import {
    type MaybeRefOrGetter,
    onWatcherCleanup,
    toValue,
    watchEffect,
    shallowReactive,
    shallowReadonly,
} from "vue";
import type { PathsWithMethod, MediaType } from "openapi-typescript-helpers";
import { onConfigChange } from "@/utils/events.ts";

export const client = createClient<paths, "application/json">({ baseUrl: "/api" });

/*
The following functions adapt the openapi-fetch promise-based interface into a reactive-based
interface.

useQuery takes a single parameter, which is one of:
* A QueryRequest object
* A reference to a QueryRequest or undefined
* A getter function that returns a QueryRequest or undefined

and returns a responsive QueryState2 object. QueryState2 is the openapi-fetch's
FetchResponse object, but with some additional properties added to track the state of
the request.

If the QueryRequest parameter ref or getter returns undefined, no
request is made and the QueryState2 object's isReady and isFetching are false. This
is useful when execution of this query should be conditional, for example if it's waiting
on the result of some other query to finish.
 */

export interface QueryRequest<
    Method extends "get",
    Path extends PathsWithMethod<paths, Method>,
    Init extends MaybeOptionalInit<paths[Path], Method>,
> {
    method: Method;
    url: Path;
    options: Init;
}

export type QueryState<
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    T extends Record<string | number, any>,
    Options,
    Media extends MediaType,
> =
    | (FetchResponse<T, Options, Media> & {
          isReady: true;
          isFetching: false;
          retry: () => void;
      })
    | {
          isReady: false;
          isFetching: boolean;
          data: undefined;
          error: undefined;
          response: undefined;
          retry: () => void;
      };

export function useQuery<
    Media extends "application/json",
    Method extends "get",
    Path extends PathsWithMethod<paths, Method>,
    Init extends MaybeOptionalInit<paths[Path], Method>,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    OP extends paths[Path][Method] & Record<string | number, any>,
>(
    getter: MaybeRefOrGetter<QueryRequest<Method, Path, Init> | undefined>,
): QueryState<OP, Init, Media> {
    const fetchResult: QueryState<OP, Init, Media> = shallowReactive({
        isReady: false,
        isFetching: false,
        data: undefined,
        error: undefined,
        response: undefined,
        retry: doRequest,
    });

    let controller: AbortController | null = null;

    function doRequest() {
        const request: QueryRequest<Method, Path, Init> | undefined = toValue(getter);
        if (request === undefined) {
            Object.assign(fetchResult, {
                isReady: false,
                isFetching: false,
                data: undefined,
                error: undefined,
                response: undefined,
            });
        } else {
            Object.assign(fetchResult, {
                isReady: false,
                isFetching: true,
                data: undefined,
                error: undefined,
                response: undefined,
            });
            if (controller) {
                controller.abort();
                controller = null;
            }
            const myController = new AbortController();
            controller = myController;
            const responsePromise = client.request(
                request.method,
                request.url,
                // @ts-expect-error the request type signature does some type magic that I can't decipher
                { ...request.options, signal: controller.signal },
            );
            responsePromise
                .then((fetchResponse) => {
                    Object.assign(fetchResult, {
                        ...fetchResponse,
                        isReady: true,
                        isFetching: false,
                    });
                })
                .catch((err) => {
                    if (err.name === "AbortError") {
                        console.debug("Request aborted");
                    } else {
                        console.error("Request errored", err);
                    }
                })
                .finally(() => {
                    if (controller === myController) {
                        controller = null;
                    }
                });
        }
    }

    watchEffect(() => {
        doRequest();
        onWatcherCleanup(() => {
            if (controller) {
                controller.abort();
                controller = null;
            }
        });
    });

    onConfigChange((event) => {
        const req = toValue(getter);
        if (req && event.url === req.url) {
            console.debug(`Config change for ${event.url}. Retrying request`);
            doRequest();
        }
    });

    return shallowReadonly(fetchResult);
}
