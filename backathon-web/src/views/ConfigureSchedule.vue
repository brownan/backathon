<template>
    <div class="panel">
        <div class="panel-block">
            <div class="content">
                <div class="field">
                    <div class="control">
                        <label class="checkbox">
                            <input
                                v-if="settings"
                                type="checkbox"
                                v-model="settings.enable"
                            />
                            Enable Schedule
                        </label>
                    </div>
                </div>
                <div class="field">
                    <label class="label">Schedule Type</label>
                    <div class="control">
                        <div class="select">
                            <select
                                v-if="settings"
                                class="select"
                                v-model="settings.mode"
                            >
                                <option value="hourly">Hourly</option>
                                <option value="daily">Daily</option>
                                <option value="weekly">Weekly</option>
                                <option value="monthly">Monthly</option>
                            </select>
                        </div>
                    </div>
                </div>
                <div class="field">
                    <label class="label">Next Backup At</label>
                    <div class="control">
                        <DatePicker
                            v-if="settings"
                            hour-format="12"
                            show-time
                            fluid
                            v-model="settings.nextRunTime"
                        />
                    </div>
                </div>
                <div class="field">
                    <button
                        type="button"
                        class="button is-primary"
                        @click="doUpdate"
                    >
                        Save
                    </button>
                </div>
            </div>
        </div>
    </div>
</template>

<style>
.schedule-info-box {
    min-height: 10rem;
}
</style>

<script setup lang="ts">
import DatePicker from "primevue/datepicker";
import { useQuery, client } from "@/api.ts";
import type { components } from "@/schema.d.ts";
import { watch, ref } from "vue";

const scheduleAPI = useQuery({
    method: "get",
    url: "/schedule",
    options: {},
});

const settings = ref<components["schemas"]["ScheduleSettings"] | null>(null);

watch(
    () => scheduleAPI.data,
    (val) => {
        if (val) {
            settings.value = val;
        }
    },
);

function doUpdate() {
    if (settings.value) {
        client.POST("/schedule", {
            body: settings.value,
        });
    }
}
</script>
