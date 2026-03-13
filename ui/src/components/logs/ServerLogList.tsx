import { useMemo, useState } from 'react';
import { useGetServerLogsQuery } from '@/store/apiSlice';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Loader2, Search, Pause, X, Info, AlertTriangle, AlertCircle, Bug } from 'lucide-react';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { formatTimestamp } from '@/lib/formatters';

const LEVEL_CONFIG: Record<string, { color: string; icon: typeof AlertCircle }> = {
  'ERROR': { color: 'text-red-500', icon: AlertCircle },
  'WARN': { color: 'text-yellow-500', icon: AlertTriangle },
  'INFO': { color: 'text-blue-500', icon: Info },
  'DEBUG': { color: 'text-gray-500', icon: Bug },
  'TRACE': { color: 'text-purple-500', icon: Bug },
};

function getLevelConfig(level: string) {
  return LEVEL_CONFIG[level.toUpperCase()] || { color: 'text-gray-500', icon: Info };
}

  export function ServerLogList() {
    const [isPaused, setIsPaused] = useState(false);
    const [searchQuery, setSearchQuery] = useState('');
    const [caseSensitive, setCaseSensitive] = useState(false);
    const [logType, setLogType] = useState('all');
  
    const {
      data: logs = [],
      isLoading,
      error: queryError,
      refetch
    } = useGetServerLogsQuery({ limit: 500 }, {
      pollingInterval: isPaused ? 0 : 2000,
    });
  
    const error = queryError && "error" in queryError && typeof queryError.error === "string" 
      ? queryError.error 
      : queryError 
        ? "Failed to fetch server logs" 
        : null;

    const filteredLogs = useMemo(() => {
    return logs.filter(log => {
      if (logType !== 'all' && log.level.toLowerCase() !== logType.toLowerCase()) return false;
      
      if (searchQuery) {
        const message = caseSensitive ? log.message : log.message.toLowerCase();
        const query = caseSensitive ? searchQuery : searchQuery.toLowerCase();
        if (!message.includes(query)) return false;
      }
      
      return true;
    });
  }, [logs, searchQuery, caseSensitive, logType]);

  // Mock data for the graph
  const hours = Array.from({ length: 24 }, (_, i) => `${i.toString().padStart(2, '0')}:00`);

  return (
    <div className="flex flex-col h-[calc(100vh-16rem)] bg-background text-foreground font-mono text-sm border rounded-md overflow-hidden shadow-sm">
      {/* Graph Section */}
      <div className="h-24 border-b p-4 flex flex-col bg-muted/30">
        <span className="text-xs text-muted-foreground mb-auto font-sans">Log Frequency Graph</span>
        <div className="flex items-end justify-between h-12 gap-1 mt-2">
          {hours.map((hour, i) => (
            <div key={hour} className="flex flex-col items-center flex-1 gap-1">
              <div 
                className={`w-full max-w-[24px] rounded-t-sm ${i === 21 ? 'bg-primary h-full' : 'bg-muted-foreground/20 h-1'}`}
              />
              <span className="text-[9px] text-muted-foreground">{hour}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Toolbar */}
      <div className="flex items-center justify-between p-3 border-b bg-muted/10 font-sans">
        <h3 className="font-medium text-sm">All Logs</h3>
        <div className="flex items-center gap-3">
          <label className="flex items-center gap-2 text-xs text-muted-foreground cursor-pointer">
            <input
              type="checkbox"
              checked={caseSensitive}
              onChange={(e) => setCaseSensitive(e.target.checked)}
              className="rounded border-input bg-transparent"
            />
            Case sensitive
          </label>
          
          <div className="relative w-64">
            <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
            <Input
              placeholder="Filter logs"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="pl-9 bg-background text-sm h-9"
            />
          </div>

          <Select value={logType} onValueChange={setLogType}>
            <SelectTrigger className="w-36 bg-background h-9 text-sm">
              <SelectValue placeholder="All Log Types" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Log Types</SelectItem>
              <SelectItem value="info">Info</SelectItem>
              <SelectItem value="warn">Warning</SelectItem>
              <SelectItem value="error">Error</SelectItem>
            </SelectContent>
          </Select>

          <Button
            variant="outline"
            size="sm"
            onClick={() => setIsPaused(!isPaused)}
            className={`h-9 ${isPaused ? 'text-primary border-primary/50' : 'text-muted-foreground'}`}
          >
            <Pause className="h-3.5 w-3.5 mr-2" />
            {isPaused ? 'RESUME' : 'PAUSE'}
          </Button>

          <Button
            variant="outline"
            size="sm"
            className="h-9 text-destructive hover:text-destructive hover:bg-destructive/10 border-destructive/20"
          >
            <X className="h-3.5 w-3.5 mr-2" />
            CLEAR LOGS
          </Button>
        </div>
      </div>

      {/* Table Header */}
      <div className="grid grid-cols-[180px_80px_150px_1fr_150px] gap-4 px-4 py-2 border-b text-xs font-semibold text-muted-foreground bg-muted/30 font-sans">
        <div>Timestamp</div>
        <div>Level</div>
        <div>Function</div>
        <div>Message</div>
        <div>File</div>
      </div>

      {/* Table Body */}
      <div className="flex-1 overflow-auto bg-background">
        {error ? (
          <div className="p-4">
            <Alert variant="destructive">
              <AlertCircle className="h-4 w-4" />
              <AlertDescription className="mt-2 space-y-3">
                <p>{error}</p>
                <Button variant="outline" onClick={() => refetch()}>
                  Retry
                </Button>
              </AlertDescription>
            </Alert>
          </div>
        ) : isLoading && filteredLogs.length === 0 ? (
          <div className="flex items-center justify-center h-full">
            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
          </div>
        ) : filteredLogs.length === 0 ? (
          <div className="flex items-center justify-center h-full text-muted-foreground font-sans">
            No logs found
          </div>
        ) : (
          <div className="flex flex-col">
            {filteredLogs.map((log, index) => {
              const levelConfig = getLevelConfig(log.level);
              const LevelIcon = levelConfig.icon;
              return (
                <div 
                  key={`${log.timestamp}-${index}`}
                  className="grid grid-cols-[180px_80px_150px_1fr_150px] gap-4 px-4 py-2 border-b border-border/50 hover:bg-muted/50 text-xs transition-colors"
                >
                  <div className="text-muted-foreground truncate">
                    {formatTimestamp(log.timestamp, 'Timestamp(Microsecond, None)')}
                  </div>
                  <div className={`flex items-center gap-1.5 ${levelConfig.color}`}>
                    <LevelIcon className="h-3 w-3" />
                    <span className="capitalize">{log.level.toLowerCase()}</span>
                  </div>
                  <div className="truncate">
                    {log.target || '__kalamdb__'}
                  </div>
                  <div className="truncate">
                    {log.message}
                  </div>
                  <div className="text-muted-foreground truncate">
                    {log.target || '__kalamdb__'}
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}
