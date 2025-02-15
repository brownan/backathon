import { onWatcherCleanup, reactive, ref, toValue, watchEffect } from "vue";
import { client } from "@/api.ts";

export function useQuery(method, url, options) {
    const data = ref(null);
    const isReady = ref(false);
    const isFetching = ref(false);
    const error = ref(null);

    async function execute(abortSignal) {
        error.value = undefined;
        isReady.value = false;
        isFetching.value = true;

        const ret = await client.request(method, url, {
            ...toValue(options),
            signal: abortSignal,
        });

        const fetchData = ret.data;
        const fetchError = ret.error;

        if (fetchError) {
            error.value = fetchError;
        } else if (fetchData) {
            data.value = fetchData;
            isReady.value = true;
        }
        isFetching.value = false;
    }

    const watchHandle = watchEffect(() => {
        const controller = new AbortController();
        execute(controller.signal);
        onWatcherCleanup(() => controller.abort());
    });

    return reactive({
        data,
        isReady,
        isFetching,
        error,
        cancel: () => watchHandle.stop(),
    });
}
