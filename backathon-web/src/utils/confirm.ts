import { useModal } from "vue-final-modal";
import ConfirmModal from "@/components/ConfirmModal.vue";

export function confirm(text: string, onConfirm: () => void) {
    const modal = useModal({
        component: ConfirmModal,
        attrs: {
            text: text,
            onConfirm: () => {
                modal.close();
                onConfirm();
            },
        },
    });
    modal.open();
}
