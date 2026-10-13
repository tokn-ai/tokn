import { useCallback, useEffect, useRef, useState } from "react";
export function useResource<T>(load: () => Promise<T>) {
  const [data, setValue] = useState<T>();
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  const generation = useRef(0);
  const setData = useCallback((value: T | ((current: T | undefined) => T)) => {
    // A saved document or pushed status supersedes any in-flight read.
    generation.current++;
    setValue(value);
    setError("");
    setLoading(false);
  }, []);
  const refresh = useCallback(async () => {
    const current = ++generation.current;
    setLoading(true);
    setError("");
    try {
      const result = await load();
      if (current === generation.current) setValue(result);
    } catch (error) {
      if (current === generation.current) setError(String(error));
    } finally {
      if (current === generation.current) setLoading(false);
    }
  }, [load]);
  useEffect(() => {
    void refresh();
    return () => {
      generation.current++;
    };
  }, [refresh]);
  return { data, setData, error, loading, refresh };
}
