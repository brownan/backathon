import { ref, toValue, watch } from "vue";

export function makeUseQuery(client) {
    function _useQuery(method, url, params) {
        const state = ref();
        const isReady = ref(false);
        const isFetching = ref(false);
        const error = ref(undefined);

        async function execute(unwrappedParams, _, onCleanup) {
            error.value = undefined;
            isReady.value = false;
            isFetching.value = true;

            const abort = new AbortController();

            const { data, error: fetchError } = await client.request(method, url, {
                ...unwrappedParams,
                signal: abort.signal,
            });

            onCleanup(abort.abort);

            if (fetchError) {
                error.value = fetchError;
            } else {
                state.value = data;
                isReady.value = true;
            }
            isFetching.value = false;
        }

        watch(
            () => toValue(params),
            (unwrappedParams) => {
                execute(unwrappedParams);
            },
            {
                immediate: true,
            },
        );
        return {
            state,
            isReady,
            isFetching,
            error,
        };
    }
    return _useQuery;
}
