<template>
    <div class="panel">
        <div class="panel-heading">Retention Settings</div>
        <div class="panel-block">
            <div class="field">
                <p class="control">
                    <label>
                        <input type="checkbox" />
                        Enable automatic snapshot deletion
                    </label>
                </p>
            </div>
        </div>
        <div
            v-for="(bucket, i) in buckets"
            :key="i"
            class="panel-block"
        >
            <button
                type="button"
                class="button mr-4 is-danger is-outlined"
            >
                <MdiIcon :path="mdiDeleteForever" />
            </button>
            <div class="field is-grouped is-align-items-center">
                <p class="control">Over the last</p>
                <p class="control">
                    <input
                        class="input"
                        style="width: 5rem"
                        v-model.number="bucket.size.count"
                    />
                </p>
                <p class="control">
                    <select
                        class="select"
                        style="min-width: 5rem"
                        v-model="bucket.size.type"
                    >
                        <option value="hourly">
                            {{ pluralize(bucket.size.count, "hour") }}
                        </option>
                        <option value="daily">
                            {{ pluralize(bucket.size.count, "day") }}
                        </option>
                        <option value="weekly">
                            {{ pluralize(bucket.size.count, "week") }}
                        </option>
                        <option value="monthly">
                            {{ pluralize(bucket.size.count, "month") }}
                        </option>
                    </select>
                </p>
                <p class="control">keep</p>
                <p class="control">
                    <input
                        class="input"
                        style="width: 5rem"
                        v-model.number="bucket.duration.count"
                    />
                </p>
                <p class="control">
                    {{ pluralize(bucket.duration.count, "backup") }} per
                </p>
                <p class="control">
                    <select
                        class="select"
                        style="min-width: 5rem"
                        v-model="bucket.duration.type"
                    >
                        <option value="hourly">hour</option>
                        <option value="daily">day</option>
                        <option value="weekly">week</option>
                        <option value="monthly">month</option>
                    </select>
                </p>
            </div>
        </div>
        <div class="panel-block">
            <button
                type="button"
                class="button"
            >
                Add new bucket
            </button>
        </div>
    </div>
</template>

<style scoped></style>

<script setup lang="ts">
import MdiIcon from "@/utils/MdiIcon.vue";
import { mdiDeleteForever } from "@mdi/js";
import { reactive } from "vue";

type IntervalType = "hourly" | "daily" | "weekly" | "monthly";
interface Interval {
    count: number;
    type: IntervalType;
}

interface Bucket {
    size: Interval;
    duration: Interval;
}

const buckets = reactive<Bucket[]>([
    {
        size: {
            count: 1,
            type: "weekly",
        },
        duration: {
            count: 1,
            type: "daily",
        },
    },
    {
        size: {
            count: 4,
            type: "weekly",
        },
        duration: {
            count: 1,
            type: "weekly",
        },
    },
    {
        size: {
            count: 12,
            type: "monthly",
        },
        duration: {
            count: 1,
            type: "monthly",
        },
    },
]);

function pluralize(num: number, singular: string, plural?: string): string {
    return num === 1 ? singular : plural ? plural : singular + "s";
}
</script>
