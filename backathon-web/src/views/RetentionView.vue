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
        <BucketRow
            v-for="(bucket, i) in buckets"
            :key="i"
            class="panel-block"
            :model-value="bucket"
        />
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
import { useQuery } from "@/api.ts";
import BucketRow from "@/components/BucketRow.vue";
import type { components } from "@/schema.d.ts";

const buckets: components["schemas"]["Bucket"][] = [
    {
        timeframe: "P1W",
        interval: "P1D",
        count: 5,
        unlimited: false,
    },
];

const retentionAPI = useQuery({
    method: "get",
    url: "/retention/settings",
    options: {},
});
</script>
