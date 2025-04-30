<template>
    <div class="panel">
        <div class="panel-block">
            Hourly
            <DatePicker
                time-only
                fulid
            />
        </div>
        <div class="panel-block">Daily</div>
    </div>
</template>

<style scoped></style>

<script setup lang="ts">
import DatePicker from "primevue/datepicker";
</script>
