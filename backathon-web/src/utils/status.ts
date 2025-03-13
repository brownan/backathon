import { defineStore } from "pinia";
import { useEventSource } from "@vueuse/core";
import type { components } from "@/schema.d.ts";
import { computed, ref, watchEffect } from "vue";

type ScanProgress = components["schemas"]["ScanProgress"];
type BackupProgress = components["schemas"]["BackupProgress"];

type EventType = {
    scan: ScanProgress | null;
    backup: BackupProgress | null;
};

export const useJobStatus = defineStore("job-status", () => {
    const { data, status, error } = useEventSource("/api/events", [], {
        autoReconnect: true,
        autoConnect: false,
        immediate: true,
    });

    const scanStatus = ref<ScanProgress | null>(null);
    const backupStatus = ref<BackupProgress | null>(null);
    const lastUpdated = ref<Date | null>(null);

    watchEffect(() => {
        if (data.value === null) {
            return;
        }
        const dataObj = JSON.parse(data.value) as EventType;
        scanStatus.value = dataObj.scan;
        backupStatus.value = dataObj.backup;
        lastUpdated.value = new Date(Date.now());
    });

    return {
        scan: computed(() => scanStatus.value),
        backup: computed(() => backupStatus.value),
        lastUpdated: computed(() => lastUpdated.value),
        connectionStatus: computed(() => status.value),
        error: computed(() => error.value),
    };
});
