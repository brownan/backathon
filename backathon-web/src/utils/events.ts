import { defineStore } from "pinia";
import { type EventSourceStatus } from "@vueuse/core";
import type { components } from "@/schema.d.ts";
import { computed, onScopeDispose, ref, shallowRef } from "vue";
import { useEventBus } from "@vueuse/core";

type ScanProgress = components["schemas"]["ScanProgress"];
type BackupProgress = components["schemas"]["BackupProgress"];
type Settings = components["schemas"]["Settings"];

interface JobStatusEvent {
    scan: ScanProgress | null;
    backup: BackupProgress | null;
}

export interface ConfigChangeEvent<K extends keyof Settings> {
    key: K;
    value: Settings[K];
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
        console.log("Event source opened");
        eventSourceStatus.value = "OPEN";
        eventSourceError.value = null;
    };

    es.onerror = (e) => {
        console.log(`Event source errored. readyState is ${es.readyState}`, e);
        eventSourceStatus.value = "CLOSED";
        eventSourceError.value = e;

        if (es.readyState === 2) {
            es.close();
            _opened = false;
            setTimeout(openEventSource, 1000);
        }
    };

    for (const en of eventNames) {
        es.addEventListener(en, (e) => {
            bus.emit({ type: en, msg: e });
        });
    }
}

export function onConfigChange(callback: (e: ConfigChangeEvent<keyof Settings>) => void) {
    openEventSource();
    const off = bus.on(({ type, msg }) => {
        if (type === "configChange") {
            callback(JSON.parse(msg.data) as ConfigChangeEvent<keyof Settings>);
        }
    });
    console.debug("Config change listener added");
    onScopeDispose(off);
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
