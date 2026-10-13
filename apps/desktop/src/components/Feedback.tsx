import { AlertCircle } from "lucide-react";

export function ErrorMessage({ error }: { error: string }) {
  return error ? (
    <div className="notice error" role="alert">
      <AlertCircle size={17} aria-hidden="true" />
      <span>{error}</span>
    </div>
  ) : null;
}
export function Empty({ children }: { children: React.ReactNode }) {
  return <div className="empty">{children}</div>;
}
export function PageHeading({
  eyebrow,
  title,
  description,
  children,
}: {
  eyebrow: string;
  title: string;
  description?: string;
  children?: React.ReactNode;
}) {
  return (
    <header className="page-heading">
      <div>
        <p className="eyebrow">{eyebrow}</p>
        <h1>{title}</h1>
        {description && <p className="page-description">{description}</p>}
      </div>
      {children}
    </header>
  );
}
export const number = (value: number) =>
  Intl.NumberFormat(undefined, {
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value);
