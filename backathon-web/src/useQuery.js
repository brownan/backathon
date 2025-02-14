import { ref, toValue, watch, watchEffect } from "vue";

export function makeUseQuery(client) {
    function _useQuery(method, url, params, disable) {
        const data = ref();
        const isReady = ref(false);
        const isFetching = ref(false);
        const error = ref(undefined);

        if (!disable) {
            disable = ref(false);
        }

        async function execute(unwrappedParams, onCleanup) {
            error.value = undefined;
            isReady.value = false;
            isFetching.value = true;

            const abort = new AbortController();
            onCleanup(abort.abort);

            const { data: fetchData, error: fetchError } = await client.request(
                method,
                url,
                {
                    ...unwrappedParams,
                    signal: abort.signal,
                },
            );

            if (fetchError) {
                error.value = fetchError;
            } else {
                data.value = fetchData;
                isReady.value = true;
            }
            isFetching.value = false;
        }

        const watchHandle = watch(
            () => toValue(params),
            (unwrappedParams, _, onCleanup) => {
                execute(unwrappedParams, onCleanup);
            },
            {
                immediate: !disable.value,
            },
        );

        watchEffect(() => {
            if (disable.value) {
                watchHandle.pause();
            } else {
                watchHandle.resume();
            }
        });

        return {
            data,
            isReady,
            isFetching,
            error,
        };
    }
    return _useQuery;
}
