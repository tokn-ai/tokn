import { useId, useLayoutEffect, useRef, type ReactNode } from "react";
import { X } from "lucide-react";

export function Modal({
  title,
  busy = false,
  onClose,
  children,
}: {
  title: string;
  busy?: boolean;
  onClose: () => void;
  children: ReactNode;
}) {
  const dialog = useRef<HTMLDialogElement>(null);
  const title_id = useId();

  useLayoutEffect(() => {
    const element = dialog.current!;
    element.showModal();
    return () => element.close();
  }, []);

  return (
    <dialog
      ref={dialog}
      className="app-modal"
      aria-labelledby={title_id}
      onCancel={(event) => {
        event.preventDefault();
        if (!busy) onClose();
      }}
    >
      <header className="modal-heading">
        <h2 id={title_id}>{title}</h2>
        <button
          type="button"
          disabled={busy}
          onClick={onClose}
          aria-label="Close dialog"
        >
          <X size={18} />
        </button>
      </header>
      {children}
    </dialog>
  );
}
