import { defineStore } from "pinia";
import { type EventSourceStatus } from "@vueuse/core";
import type { components } from "@/schema.d.ts";
import { computed, onScopeDispose, ref, shallowRef } from "vue";
import { useEventBus } from "@vueuse/core";

type ScanProgress = components["schemas"]["ScanProgress"];
type BackupProgress = components["schemas"]["BackupProgress"];

interface JobStatusEvent {
    scan: ScanProgress | null;
    backup: BackupProgress | null;
}

interface ConfigChangeEvent {
    url: string;
}

const eventNames = ["statusUpdate", "configChange"];

const bus = useEventBus<{ type: string; msg: MessageEvent }>("event-stream-bus");

export const eventSourceStatus = shallowRef<EventSourceStatus>("CLOSED");
export const eventSourceError = shallowRef<Event | null>(null);

let _opened = false;

function openEventSource() {
    if (_opened) {
        return;
    }
    _opened = true;
    console.log("Connecting to event source");

    const es = new EventSource("/api/events");

    eventSourceStatus.value = "CONNECTING";

    es.onopen = () => {
        eventSourceStatus.value = "OPEN";
        eventSourceError.value = null;
    };

    es.onerror = (e) => {
        eventSourceStatus.value = "CLOSED";
        eventSourceError.value = e;

        if (es.readyState === 2) {
            es.close();
            setTimeout(openEventSource, 1000);
        }
    };

    for (const en of eventNames) {
        es.addEventListener(en, (e) => {
            bus.emit({ type: en, msg: e });
        });
    }
}

export function onConfigChange(callback: (e: ConfigChangeEvent) => void) {
    openEventSource();
    const off = bus.on(({ type, msg }) => {
        if (type === "configChange") {
            console.log("Config change event. notifying listener");
            callback(JSON.parse(msg.data) as ConfigChangeEvent);
        }
    });
    console.debug("Config change listener added");
    onScopeDispose(() => {
        console.debug("Config change listener removed due to scope disposal");
        off();
    });
}

export const useJobStatus = defineStore("job-status", () => {
    openEventSource();

    const scanStatus = ref<ScanProgress | null>(null);
    const backupStatus = ref<BackupProgress | null>(null);
    const lastUpdated = ref<Date | null>(null);

    const off = bus.on(({ type, msg }) => {
        if (type === "statusUpdate") {
            const dataObj = JSON.parse(msg.data) as JobStatusEvent;
            console.debug("New status updated:", dataObj);

            scanStatus.value = dataObj.scan;
            backupStatus.value = dataObj.backup;
            lastUpdated.value = new Date(Date.now());
        }
    });

    onScopeDispose(off);

    return {
        scan: computed(() => scanStatus.value),
        backup: computed(() => backupStatus.value),
        lastUpdated: computed(() => lastUpdated.value),
        connectionStatus: computed(() => eventSourceStatus.value),
        error: computed(() => eventSourceError.value),
    };
});
