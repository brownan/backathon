import createClient from "openapi-fetch";

import type { paths } from "./schema.d.ts";
import { computed, onWatcherCleanup, reactive, type Ref, ref, watchEffect } from "vue";
import type { QueryState } from "@/useQuery";

export const client = createClient<paths, "application/json">({ baseUrl: "/api" });

export { useQuery } from "@/useQuery";

function destructureQueryStateRef<T, E>(
    queryStateRef: Ref<QueryState<T, E> | undefined>,
): QueryState<T, E> {
    return reactive({
        data: computed(() => queryStateRef.value?.data || null),
        isReady: computed(() => queryStateRef.value?.isReady || false),
        isFetching: computed(() => queryStateRef.value?.isFetching || false),
        error: computed(() => queryStateRef.value?.error || null),
        cancel: () => queryStateRef.value?.cancel(),
    });
}

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
    return destructureQueryStateRef(queryStateRef);
}
