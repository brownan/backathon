import { reactive, ref, toValue, watch } from "vue";
import { client } from "@/api.ts";

/**
 * Takes an object and passes each key through vue's toValue() to unwrap
 * it if it's a reference, computed ref, or getter function
 * @param params Record<string, MaybeRefOrGetter<any>>
 * @returns {{[p: string]: any}}
 */
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
    let { params, ...otherOptions } = options;

    params = params ? params : {};

    async function execute(unwrappedParams, onCleanup) {
        error.value = undefined;
        isReady.value = false;
        isFetching.value = true;

        const abort = new AbortController();
        onCleanup(abort.abort);

        const ret = await client.request(method, url, {
            ...{ params: unwrappedParams },
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

    const watchHandle = watch(
        () => unwrapParams(params),
        (unwrappedParams, _, onCleanup) => execute(unwrappedParams, onCleanup),
        {
            immediate: true,
        },
    );

    return reactive({
        data,
        isReady,
        isFetching,
        error,
        stop: () => watchHandle.stop(),
    });
}
