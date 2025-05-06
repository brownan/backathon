<template>
    <div>
        <button
            type="button"
            class="button mr-4 is-danger is-outlined"
            @click="$emit('delete')"
        >
            <MdiIcon :path="mdiDeleteForever" />
        </button>
        <div class="field is-grouped is-align-items-center">
            <p class="control">Over the last</p>
            <p class="control">
                <input
                    class="input"
                    style="width: 5rem"
                    v-model.number="timeframeCount"
                />
            </p>
            <p class="control">
                <select
                    class="select"
                    style="min-width: 5rem"
                    v-model="timeframeType"
                >
                    <option value="hourly">
                        {{ pluralize(timeframeCount, "hour") }}
                    </option>
                    <option value="daily">
                        {{ pluralize(timeframeCount, "day") }}
                    </option>
                    <option value="weekly">
                        {{ pluralize(timeframeCount, "week") }}
                    </option>
                    <option value="monthly">
                        {{ pluralize(timeframeCount, "month") }}
                    </option>
                </select>
            </p>
            <p class="control">keep</p>
            <p class="control">
                <input
                    class="input"
                    style="width: 5rem"
                    v-model.number="bucket.count"
                />
            </p>
            <p class="control">{{ pluralize(bucket.count, "backup") }} per</p>
            <p class="control">
                <select
                    class="select"
                    style="min-width: 5rem"
                    v-model="intervalType"
                >
                    <option value="hourly">hour</option>
                    <option value="daily">day</option>
                    <option value="weekly">week</option>
                    <option value="monthly">month</option>
                </select>
            </p>
            <p>
                (or
                <label
                    ><input
                        type="checkbox"
                        class="checkbox"
                        v-model="bucket.unlimited"
                    />unlimited</label
                >)
            </p>
        </div>
    </div>
</template>

<style scoped></style>

<script setup lang="ts">
import MdiIcon from "@/utils/MdiIcon.vue";
import { mdiDeleteForever } from "@mdi/js";
import type { components } from "@/schema.d.ts";
import { computed, triggerRef } from "vue";

defineEmits(["delete"]);

type DurationType = "hourly" | "daily" | "weekly" | "monthly";

const bucket = defineModel<components["schemas"]["Bucket"]>({ required: true });

const timeframe = computed({
    get() {
        return bucket.value.timeframe;
    },
    set(val: string) {
        // Mutate the model prop
        bucket.value.timeframe = val;
        triggerRef(bucket);
    },
});
const interval = computed({
    get() {
        return bucket.value.interval;
    },
    set(val: string) {
        // Mutate the model prop
        bucket.value.interval = val;
        triggerRef(bucket);
    },
});

const timeframeDuration = computed({
    get() {
        return strToDuration(timeframe.value);
    },
    set(d: Duration) {
        timeframe.value = durationToStr(d);
    },
});

const intervalDuration = computed({
    get() {
        return strToDuration(interval.value);
    },
    set(d: Duration) {
        interval.value = durationToStr(d);
    },
});

const timeframeCount = computed({
    get() {
        return timeframeDuration.value.count;
    },
    set(val: number) {
        timeframeDuration.value = { count: val, type: timeframeDuration.value.type };
    },
});

const timeframeType = computed({
    get() {
        return timeframeDuration.value.type;
    },
    set(val: DurationType) {
        timeframeDuration.value = { count: timeframeDuration.value.count, type: val };
    },
});

const intervalType = computed({
    get() {
        return intervalDuration.value.type;
    },
    set(val: DurationType) {
        intervalDuration.value = { count: 1, type: val };
    },
});

interface Duration {
    count: number;
    type: DurationType;
}
function pluralize(num: number, singular: string, plural?: string): string {
    return num === 1 ? singular : plural ? plural : singular + "s";
}

function strToDuration(durStr: string): Duration {
    let match;
    if ((match = /P(\d+)M/.exec(durStr))) {
        return { count: Number.parseInt(match[1]), type: "monthly" };
    } else if ((match = /P(\d+)W/.exec(durStr))) {
        return { count: Number.parseInt(match[1]), type: "weekly" };
    } else if ((match = /P(\d+)D/.exec(durStr))) {
        return { count: Number.parseInt(match[1]), type: "daily" };
    } else if ((match = /PT(\d+)H/.exec(durStr))) {
        return { count: Number.parseInt(match[1]), type: "hourly" };
    }
    return { count: 0, type: "hourly" };
}

function durationToStr(val: Duration): string {
    if (!val.count) {
        return "";
    } else if (val.type === "monthly") {
        return `P${val.count}M`;
    } else if (val.type === "weekly") {
        return `P${val.count}W`;
    } else if (val.type === "daily") {
        return `P${val.count}D`;
    } else if (val.type === "hourly") {
        return `PT${val.count}H`;
    } else {
        return "";
    }
}
</script>
