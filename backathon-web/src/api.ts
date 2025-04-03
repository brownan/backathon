import createClient from "openapi-fetch";

import type { paths } from "./schema.d.ts";
import { computed, onWatcherCleanup, reactive, type Ref, ref, watchEffect } from "vue";
import type { QueryState } from "@/useQuery";

export const client = createClient<paths, "application/json">({ baseUrl: "/api" });

export { useQuery } from "@/useQuery";

/*
The useQuery and conditionalUseQuery functions are my attempts at adapting
the promise-based interface of openapi-fetch with vue's reactivity.

---

useQuery()
exposes a basic reactive interface to an openapi request.

useQuery(method, url, options) returns a reactive object with the following properties:
- data: the data returned from the query
- isReady: a boolean for if the data has been fetched and is set
- isFetching: a fetch is currently pending
- error: the latest query returned an error
- cancel(): a function that stops any pending queries and stops the reactive watchers on
  the parameters

The function reactively watches the options parameter, which can be an object, ref, or
getter.

---

conditionalUseQuery()
The problem with useQuery() is that the options to perform the fetch may not be
immediately available (for example, if it depends on a previous query to finish).
Passing null or undefined in for options isn't valid with the typing of the useQuery()
interface, and a goal was to keep the strong typing of the option parameters from
the openapi-fetch library.

So conditionalUseQuery() acts as a wrapper for useQuery(). It takes a getter function
that is expected to call useQuery() and return the resulting QueryState object, or
return undefined.

The result is a QueryState object the same as useQuery(), but if a query wasn't made,
isReady will be false and data will be null.

Intended usage:

const { data: rootObj } = toRefs(
    conditionalUseQuery(() => {
        if (someRef.value) {
            return useQuery("get", "/SomeAPI/{someParameter}", {
                params: {
                    path: {
                        someParameter: someRef.value,
                    },
                },
            });
        }
    }),
);

The call to toRefs() is necessary when destructuring a value from a reactive object.

This pattern preserves the strong type checking of the options parameter in useQuery()
while still allowing queries to be conditionally performed.

 */

export function conditionalUseQuery<T, E>(
    fn: () => QueryState<T, E> | undefined,
): QueryState<T, E> {
    const queryStateRef: Ref<QueryState<T, E> | undefined> = ref(undefined);
    watchEffect(() => {
        const maybeQueryState = fn();
        queryStateRef.value = maybeQueryState;
        if (maybeQueryState) {
            onWatcherCleanup(() => maybeQueryState.cancel());
        }
    });

    return reactive({
        data: computed(() => queryStateRef.value?.data || null),
        isReady: computed(() => queryStateRef.value?.isReady || false),
        isFetching: computed(() => queryStateRef.value?.isFetching || false),
        error: computed(() => queryStateRef.value?.error || null),
        cancel: () => queryStateRef.value?.cancel(),
    });
}
