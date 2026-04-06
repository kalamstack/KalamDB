/**
 * Datatype Display Components
 * 
 * Provides specialized rendering for each KalamDB datatype.
 * Each component handles formatting, styling, and interaction for its specific type.
 */

import { KalamCellValue } from '@kalamdb/client';
import { TimestampDisplay } from './TimestampDisplay';
import { DateDisplay } from './DateDisplay';
import { FileDisplay } from './FileDisplay';
import { JsonDisplay } from './JsonDisplay';
import { BooleanDisplay } from './BooleanDisplay';
import { NumberDisplay } from './NumberDisplay';
import { TextDisplay } from './TextDisplay';

export { TimestampDisplay, DateDisplay, FileDisplay, JsonDisplay, BooleanDisplay, NumberDisplay, TextDisplay };

/**
 * Main dispatcher component that routes to the appropriate display component
 * based on the column's data type.
 */
interface CellDisplayProps {
  value: unknown;
  dataType?: string;
  namespace?: string;
  tableName?: string;
}

export function CellDisplay({ value, dataType, namespace, tableName }: CellDisplayProps) {
  // Unwrap KalamCellValue to get the raw underlying JSON value.
  // This makes CellDisplay work transparently with both legacy raw values
  // and the new typed KalamCellValue wrappers from rowsToObjects().
  const rawValue = value instanceof KalamCellValue ? value.toJson() : value;

  // Handle null/undefined
  if (rawValue === null || rawValue === undefined) {
    return <span className="text-muted-foreground italic">null</span>;
  }

  // Normalize dataType for comparison
  const normalizedType = dataType?.toUpperCase() || '';
  
  // Route to appropriate display component based on dataType
  // Handle Timestamp types (including Arrow types)
  if (normalizedType.startsWith('TIMESTAMP') || normalizedType === 'DATETIME') {
    return <TimestampDisplay value={rawValue as number | string} dataType={dataType} />;
  }
  
  // Handle Date types
  if (normalizedType === 'DATE' || normalizedType.startsWith('DATE32') || normalizedType.startsWith('DATE64')) {
    const dateValue = typeof rawValue === 'number' || typeof rawValue === 'string'
      ? rawValue
      : String(rawValue);
    return <DateDisplay value={dateValue} />;
  }
  
  // Handle Boolean
  if (normalizedType === 'BOOLEAN' || normalizedType === 'BOOL') {
    const boolValue = typeof rawValue === 'boolean'
      ? rawValue
      : String(rawValue).toLowerCase() === 'true';
    return <BooleanDisplay value={boolValue} />;
  }
  
  // Handle Numeric types (including Arrow types)
  if (
    // Standard SQL types
    normalizedType === 'INT' || normalizedType === 'INTEGER' ||
    normalizedType === 'BIGINT' || 
    normalizedType === 'SMALLINT' || normalizedType === 'TINYINT' ||
    normalizedType === 'FLOAT' || normalizedType === 'DOUBLE' || normalizedType === 'DECIMAL' ||
    // Arrow types
    normalizedType.startsWith('INT8') || normalizedType.startsWith('INT16') || 
    normalizedType.startsWith('INT32') || normalizedType.startsWith('INT64') ||
    normalizedType.startsWith('UINT8') || normalizedType.startsWith('UINT16') || 
    normalizedType.startsWith('UINT32') || normalizedType.startsWith('UINT64') ||
    normalizedType.startsWith('FLOAT16') || normalizedType.startsWith('FLOAT32') || 
    normalizedType.startsWith('FLOAT64')
  ) {
    // Map Arrow types to display types
    let displayType: 'INT' | 'BIGINT' | 'SMALLINT' | 'FLOAT' | 'DOUBLE' | 'DECIMAL' = 'INT';
    
    if (normalizedType.includes('64') || normalizedType === 'BIGINT') {
      displayType = 'BIGINT';
    } else if (normalizedType.includes('FLOAT') || normalizedType.includes('DOUBLE')) {
      displayType = 'DOUBLE';
    } else if (normalizedType === 'DECIMAL') {
      displayType = 'DECIMAL';
    } else if (normalizedType.includes('16') || normalizedType.includes('8') || normalizedType === 'SMALLINT') {
      displayType = 'SMALLINT';
    }

    if (displayType === 'BIGINT' && (typeof rawValue === 'string' || typeof rawValue === 'bigint')) {
      return <NumberDisplay value={rawValue} dataType={displayType} />;
    }
    
    const numericValue =
      typeof rawValue === 'number'
        ? rawValue
        : Number(rawValue);
    if (Number.isNaN(numericValue)) {
      return <TextDisplay value={String(rawValue)} />;
    }
    return <NumberDisplay value={numericValue} dataType={displayType} />;
  }
  
  // Handle File type
  if (normalizedType === 'FILE') {
    return <FileDisplay value={rawValue} namespace={namespace} tableName={tableName} />;
  }
  
  // Handle JSON type
  if (normalizedType === 'JSON') {
    return <JsonDisplay value={rawValue} />;
  }
  
  // Handle Text types
  if (normalizedType === 'TEXT' || normalizedType === 'STRING' || normalizedType === 'VARCHAR' || 
      normalizedType.startsWith('UTF8') || normalizedType === 'LARGESTRING') {
    return <TextDisplay value={String(rawValue)} />;
  }
  
  // Fallback for unknown types
  if (typeof rawValue === 'object') {
    return <JsonDisplay value={rawValue} />;
  }
  
  return <TextDisplay value={String(rawValue)} />;
}
