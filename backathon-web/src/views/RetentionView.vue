<template>
    <div class="panel">
        <div class="panel-heading">Retention Settings</div>
        <div class="panel-block">
            <div class="field">
                <p class="control">
                    <label>
                        <input
                            v-if="settings"
                            type="checkbox"
                            v-model="settings.enabled"
                        />
                        Enable automatic snapshot deletion
                    </label>
                </p>
            </div>
        </div>
        <div
            v-if="retentionAPI.isFetching"
            class="panel-block"
        >
            <SpinnerIcon />
        </div>
        <BucketRow
            v-else
            v-for="(bucket, i) in settings ? settings.buckets : []"
            :key="i"
            class="panel-block"
            :model-value="bucket"
            @delete="removeRow(i)"
        />
        <div class="panel-block">
            <div class="field is-grouped">
                <button
                    type="button"
                    class="control button"
                    @click="addNew"
                >
                    Add new bucket
                </button>
                <button
                    type="button"
                    class="control button is-primary"
                    @click="doSave"
                >
                    Save
                </button>
            </div>
        </div>
    </div>
</template>

<style scoped></style>

<script setup lang="ts">
import { useQuery, client } from "@/api.ts";
import BucketRow from "@/components/BucketRow.vue";
import SpinnerIcon from "@/components/SpinnerIcon.vue";
import type { components } from "@/schema.d.ts";
import { ref, watch } from "vue";

const retentionAPI = useQuery({
    method: "get",
    url: "/retention/settings",
    options: {},
});

const settings = ref<components["schemas"]["RetentionSettings"] | null>(null);

watch(
    () => retentionAPI.data,
    (val) => {
        if (val) {
            settings.value = val;
        }
    },
);

function addNew() {
    if (settings.value) {
        settings.value.buckets.push({
            timeframe: "P1D",
            interval: "PT1H",
            count: 1,
            unlimited: false,
        });
    }
}

function doSave() {
    if (settings.value) {
        client.POST("/retention/settings", {
            body: settings.value,
        });
    }
}

function removeRow(index: number) {
    if (settings.value) {
        settings.value.buckets.splice(index, 1);
    }
}
</script>
