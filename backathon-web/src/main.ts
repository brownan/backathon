import { createApp } from "vue";
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

app.mount("#app");
