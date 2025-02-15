import createClient from "openapi-fetch";

import type { paths } from "./schema.d.ts";
import type { Ref } from "vue";
import type { QueryState } from "@/useQuery";
import { computed, reactive, ref, watch } from "vue";

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
        stop: () => queryStateRef.value?.stop(),
    });
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
