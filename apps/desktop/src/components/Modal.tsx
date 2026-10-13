import {
  useId,
  useLayoutEffect,
  useRef,
  type ReactNode,
  type RefObject,
} from "react";
import { X } from "lucide-react";

export function Modal({
  title,
  description,
  class_name = "",
  return_focus,
  busy = false,
  onClose,
  children,
}: {
  title: string;
  description?: string;
  class_name?: string;
  return_focus?: RefObject<HTMLElement | null>;
  busy?: boolean;
  onClose: () => void;
  children: ReactNode;
}) {
  const dialog = useRef<HTMLDialogElement>(null);
  const title_id = useId();
  const description_id = useId();
  const backdrop_press = useRef(false);

  useLayoutEffect(() => {
    const element = dialog.current!;
    const previous_focus = document.activeElement;
    const previous_overflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    element.showModal();
    return () => {
      element.close();
      document.body.style.overflow = previous_overflow;
      if (previous_focus instanceof HTMLElement && previous_focus.isConnected)
        previous_focus.focus({ preventScroll: true });
      if (document.activeElement === document.body)
        return_focus?.current?.focus({ preventScroll: true });
    };
  }, [return_focus]);

  return (
    <dialog
      ref={dialog}
      className={`app-modal ${class_name}`}
      aria-labelledby={title_id}
      aria-describedby={description ? description_id : undefined}
      onPointerDown={(event) => {
        const bounds = event.currentTarget.getBoundingClientRect();
        backdrop_press.current =
          event.target === event.currentTarget &&
          (event.clientX < bounds.left ||
            event.clientX > bounds.right ||
            event.clientY < bounds.top ||
            event.clientY > bounds.bottom);
      }}
      onClick={(event) => {
        const bounds = event.currentTarget.getBoundingClientRect();
        if (
          backdrop_press.current &&
          event.target === event.currentTarget &&
          (event.clientX < bounds.left ||
            event.clientX > bounds.right ||
            event.clientY < bounds.top ||
            event.clientY > bounds.bottom) &&
          !busy
        )
          onClose();
        backdrop_press.current = false;
      }}
      onCancel={(event) => {
        event.preventDefault();
        if (!busy) onClose();
      }}
    >
      <header className="modal-heading">
        <div>
          <h2 id={title_id}>{title}</h2>
          {description && (
            <p id={description_id} className="modal-description">
              {description}
            </p>
          )}
        </div>
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
