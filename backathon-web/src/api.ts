import createClient from "openapi-fetch";

import type { paths } from "./schema.d.ts";
import type { Ref } from "vue";
import type { QueryState } from "@/useQuery";
import { computed, ref, watch } from "vue";

export const client = createClient<paths, "application/json">({ baseUrl: "/api" });

export { useQuery } from "@/useQuery";

export function destructureQueryStateRef<T, E>(
    queryStateRef: Ref<QueryState<T, E> | undefined>,
): QueryState<T, E> {
    return {
        data: computed(() => queryStateRef.value?.data.value || null),
        isReady: computed(() => queryStateRef.value?.isReady.value || false),
        isFetching: computed(() => queryStateRef.value?.isFetching.value || false),
        error: computed(() => queryStateRef.value?.error.value || null),
        stop: () => queryStateRef.value?.stop(),
    };
}

export function conditionalUseQuery<T, E>(
    getter: () => QueryState<T, E> | undefined,
): QueryState<T, E> {
    const currentState: Ref<QueryState<T, E> | undefined> = ref(undefined);
    watch(
        getter,
        (newval, oldval) => {
            if (oldval && oldval !== newval) {
                oldval.stop();
            }
            currentState.value = newval;
        },
        { immediate: true },
    );
    return destructureQueryStateRef(currentState);
}
