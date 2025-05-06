<template>
    <div class="message">
        <div class="message-header">Schedule Info</div>
        <div class="message-body schedule-info-box">foo</div>
    </div>
    <div class="panel">
        <div class="panel-block">
            <div class="content">
                <div class="field">
                    <div class="control">
                        <label class="checkbox">
                            <input type="checkbox" />
                            Enable Schedule
                        </label>
                    </div>
                </div>
                <div class="field">
                    <label class="label">Schedule Type</label>
                    <div class="control">
                        <div class="select">
                            <select>
                                <option>Hourly</option>
                                <option>Daily</option>
                                <option>Weekly</option>
                                <option>Monthly</option>
                            </select>
                        </div>
                    </div>
                </div>
                <div class="field">
                    <label class="label">Next Backup At</label>
                    <div class="control">
                        <DatePicker
                            hour-format="12"
                            show-time
                            fluid
                        />
                    </div>
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
</script>
