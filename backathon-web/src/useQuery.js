import { ref } from "vue";

export function makeUseQuery(client) {
    function _useQuery(method, url, params) {
        const state = ref();
        const isReady = ref(false);
        const isFetching = ref(false);
        const error = ref(undefined);

        async function execute() {
            error.value = undefined;
            isReady.value = false;
            isFetching.value = true;

            const { data, error: fetchError } = await client.request(method, url, params);

            if (fetchError) {
                error.value = fetchError;
            } else {
                state.value = data;
                isReady.value = true;
            }
            isFetching.value = false;
        }
        execute();
        return {
            state,
            isReady,
            isFetching,
            error,
            execute,
        };
    }
    return _useQuery;
}

export const useQuery = _useQuery;
