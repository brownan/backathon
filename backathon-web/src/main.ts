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
            root: "checkbox",
            input: "checkbox-input",
            box: "checkbox-box",
        },
        Button: {
            root: "button",
            label: ({ instance, props }) => {
                if (instance.hasIcon && !props.label && !props.badge) {
                    return {
                        style: "visibility: hidden; width: 0",
                    };
                }
            },
        },
        DatePicker: {
            panel: "datepicker-panel",
            pcInputText: {
                root: "input",
            },
            timePicker: "datepicker-timepicker",
        },
    },
});

app.mount("#app");
