import { defineStore } from "pinia";
import { client, useQuery } from "@/api.ts";
import type { components } from "@/schema.d.ts";
import { computed, watchEffect } from "vue";
import { type ConfigChangeEvent, onConfigChange } from "@/utils/events.ts";

type Settings = components["schemas"]["Settings"];

export const Settings = defineStore("settings", () => {
    const query = useQuery({
        method: "get",
        url: "/settings",
        options: {},
    });

    onConfigChange(<K extends keyof Settings>(e: ConfigChangeEvent<K>) => {
        if (query.isReady && query.data) {
            query.data[e.key] = e.value;
        }
    });

    watchEffect(() => {
        if (!query.isFetching && !query.isReady && query.error) {
            console.warn("Settings failed to fetch. Retrying", query.error);
            setTimeout(() => query.retry(), 5000);
        }
    });

    function setSetting<K extends keyof Settings>(key: K, value: Settings[K]) {
        if (query.isReady && query.data) {
            query.data[key] = value;
        }
        client.POST("/settings/{key}", {
            params: {
                path: {
                    key,
                },
            },
            body: JSON.stringify(value),
        });
    }

    return {
        settings: computed(() => query.data),
        isReady: computed(() => query.isReady),
        isFetching: computed(() => query.isFetching),
        error: computed(() => query.error),
        setSetting,
    };
});
