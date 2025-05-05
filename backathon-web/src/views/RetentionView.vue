<template>
    <div class="panel">
        <div class="panel-heading">Retention Settings</div>
        <div class="panel-block">
            <button
                type="button"
                class="button mr-4 is-danger is-outlined"
            >
                <MdiIcon :path="mdiDeleteForever" />
            </button>
            <div
                v-for="(bucket, i) in buckets"
                :key="i"
                class="field is-grouped is-align-items-center"
            >
                <p class="control">Over the last</p>
                <p class="control">
                    <input
                        class="input"
                        v-model.number="bucket.size.count"
                    />
                </p>
                <p class="control">
                    <select
                        class="select"
                        v-model="bucket.size.type"
                    >
                        <option value="hourly">Hourly</option>
                        <option value="daily">Daily</option>
                        <option value="weekly">Weeks</option>
                        <option value="monthly">Monthly</option>
                    </select>
                </p>
                <p class="control">keep</p>
                <p class="control">
                    <input
                        class="input"
                        v-model.number="bucket.number.count"
                    />
                </p>
                <p class="control">
                    <select
                        class="select"
                        v-model="bucket.number.type"
                    >
                        <option value="hourly">Hourly</option>
                        <option value="daily">Daily</option>
                        <option value="weekly">Weeks</option>
                        <option value="monthly">Monthly</option>
                    </select>
                </p>
                <p class="control">backups</p>
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
    number: Interval;
}

const buckets = reactive<Bucket[]>([
    {
        size: {
            count: 5,
            type: "weekly",
        },
        number: {
            count: 2,
            type: "hourly",
        },
    },
]);
</script>
