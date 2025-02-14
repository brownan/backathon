import { ref, toValue, watch } from "vue";
import { client } from "@/api.js";

function unwrapParams(params) {
    return Object.fromEntries(
        Object.keys(params).map((key) => [key, toValue(params[key])]),
    );
}

export function useQuery(method, url, options) {
    const data = ref(null);
    const isReady = ref(false);
    const isFetching = ref(false);
    const error = ref(null);

    // Build the fetch options object with everything but the params, which
    // may be reactive and therefore we resolve below.
    // Also pull out any options that won't be passed thru
    // eslint-disable-next-line no-unused-vars,@typescript-eslint/no-unused-vars
    const { enable, params, ...otherOptions } = options;

    async function execute(unwrappedParams, onCleanup) {
        error.value = undefined;
        isReady.value = false;
        isFetching.value = true;

        const abort = new AbortController();
        onCleanup(abort.abort);

        const ret = await client.request(method, url, {
            ...unwrappedParams,
            ...otherOptions,
            signal: abort.signal,
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

    watch(
        () => unwrapParams(params),
        (unwrappedParams, _, onCleanup) => execute(unwrappedParams, onCleanup),
        {
            immediate: true,
        },
    );

    return {
        data,
        isReady,
        isFetching,
        error,
    };
}
