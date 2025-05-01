import { createApp } from "vue";
import { createPinia } from "pinia";
import PrimeVue from "primevue/config";
import App from "./App.vue";
import router from "./router.ts";

import "@/styles.scss";

const app = createApp(App);

app.use(router);

// modal stuff
import { createVfm } from "vue-final-modal";
const vfm = createVfm();
app.use(vfm);
import "vue-final-modal/style.css";
import type { AutoCompleteContext, AutoCompleteState } from "primevue/autocomplete";

// vue store
const pinia = createPinia();
app.use(pinia);

// PrimeVue
app.use(PrimeVue, {
    unstyled: true,
    pt: {
        InputText: { root: "input" },
        AutoComplete: {
            overlay: "dropdown dropdown-content",
            option: (args: {
                context: AutoCompleteContext;
                state: AutoCompleteState;
            }) => {
                return {
                    class: { "dropdown-item": true, "is-active": args.context.focused },
                };
            },
        },
        Checkbox: {
            root: "p-checkbox",
            input: "p-checkbox-input",
            box: "p-checkbox-box",
        },
        Button: {
            root: "button",
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            label: ({ instance, props }: { [key: string]: any }) => {
                if (instance.hasIcon && !props.label && !props.badge) {
                    return {
                        style: "visibility: hidden; width: 0",
                    };
                }
            },
        },
        DatePicker: {
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            root: ({ props }: { props: any }) => ({ class: { "is-flex": props.fluid } }),
            panel: "datepicker-panel",
            pcInputText: {
                root: "input",
            },
            timePicker: "datepicker-timepicker",
            calendarContainer: "datepicker-calendar-container",
            calendar: "datepicker-calendar",
            header: "datepicker-header",
            dayView: "datepicker-day-view",
            weekHeader: "datepicker-weekday-cell",
            weekHeaderLabel: "datepicker-weekday",
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            dayCell: ({ context }: { context: any }) => [
                "datepicker-day-cell",
                {
                    "datepicker-other-month": context.otherMonth,
                    "datepicker-today": context.today,
                },
            ],
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            day: ({ context }: { context: any }) => [
                "datepicker-day",
                {
                    "is-disabled": context.disabled,
                    "is-selected": context.selected,
                },
            ],
        },
    },
});

app.mount("#app");
