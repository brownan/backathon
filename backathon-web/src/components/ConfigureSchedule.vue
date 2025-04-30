<template>
    <div class="panel">
        <div class="panel-block">
            <div class="field">
                <label class="label">Every hour at</label>
                <p class="control">
                    <InputNumber
                        :max="59"
                        :min="0"
                    />
                </p>
            </div>
        </div>
        <div class="panel-block">
            <div class="field">
                <label class="label">Every day at</label>
                <p class="control">
                    <DatePicker
                        :time-only="true"
                        :fluid="true"
                    />
                </p>
            </div>
        </div>
    </div>
</template>

<style scoped></style>

<script setup lang="ts">
import DatePicker from "primevue/datepicker";
import InputNumber from "primevue/inputnumber";
</script>
