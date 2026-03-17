interface NumberDisplayProps {
  value: number | string | bigint;
  dataType?: 'INT' | 'BIGINT' | 'SMALLINT' | 'FLOAT' | 'DOUBLE' | 'DECIMAL';
}

export function NumberDisplay({ value, dataType }: NumberDisplayProps) {
  if (value === null || value === undefined) {
    return <span className="text-muted-foreground italic">null</span>;
  }

  // Format based on data type
  let formatted: string;
  if (dataType === 'FLOAT' || dataType === 'DOUBLE' || dataType === 'DECIMAL') {
    formatted = Number(value).toLocaleString(undefined, {
      minimumFractionDigits: 0,
      maximumFractionDigits: 6,
    });
  } else if (dataType === 'BIGINT') {
    // Preserve the exact textual representation for 64-bit integers.
    formatted = String(value);
  } else {
    formatted = Number(value).toLocaleString();
  }

  return (
    <span className="font-mono text-sm tabular-nums" title={String(value)}>
      {formatted}
    </span>
  );
}
