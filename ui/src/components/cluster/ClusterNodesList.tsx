import { useState } from 'react';
import type { ClusterNode } from '@/services/clusterService';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Search, RefreshCw, Loader2, Crown, CheckCircle2, Server } from 'lucide-react';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip';

interface ClusterNodesListProps {
  nodes: ClusterNode[];
  isLoading: boolean;
  onRefresh: () => void;
}

export function ClusterNodesList({ nodes, isLoading, onRefresh }: ClusterNodesListProps) {
  const [searchQuery, setSearchQuery] = useState('');

  const filteredNodes = nodes.filter(node => {
    if (!searchQuery.trim()) return true;
    const query = searchQuery.toLowerCase();
    return (
      node.node_id.toString().includes(query) ||
      node.role.toLowerCase().includes(query) ||
      node.status.toLowerCase().includes(query) ||
      (node.hostname?.toLowerCase().includes(query) ?? false) ||
      node.api_addr.toLowerCase().includes(query) ||
      node.rpc_addr.toLowerCase().includes(query)
    );
  });

  const getStatusBadge = (status: string) => {
    switch (status.toLowerCase()) {
      case 'active':
        return <Badge className="bg-green-100 text-green-800 hover:bg-green-100">Active</Badge>;
      case 'offline':
        return <Badge className="bg-red-100 text-red-800 hover:bg-red-100">Offline</Badge>;
      case 'joining':
        return <Badge className="bg-yellow-100 text-yellow-800 hover:bg-yellow-100">Joining</Badge>;
      case 'catching_up':
        return <Badge className="bg-orange-100 text-orange-800 hover:bg-orange-100">Catching Up</Badge>;
      default:
        return <Badge variant="secondary">{status}</Badge>;
    }
  };

  const getRoleBadge = (role: string) => {
    switch (role.toLowerCase()) {
      case 'leader':
        return <Badge className="bg-blue-100 text-blue-800 hover:bg-blue-100">Leader</Badge>;
      case 'follower':
        return <Badge variant="secondary">Follower</Badge>;
      case 'learner':
        return <Badge className="bg-purple-100 text-purple-800 hover:bg-purple-100">Learner</Badge>;
      case 'candidate':
        return <Badge className="bg-yellow-100 text-yellow-800 hover:bg-yellow-100">Candidate</Badge>;
      default:
        return <Badge variant="outline">{role}</Badge>;
    }
  };

  const formatNumber = (num: number | null) => {
    if (num === null) return '—';
    return num.toLocaleString();
  };

  const formatMemory = (memoryMb: number | null) => {
    if (memoryMb === null) return '—';
    const precision = memoryMb >= 100 ? 0 : 1;
    return `${memoryMb.toFixed(precision)} MB`;
  };

  const formatCpu = (cpuPercent: number | null) => {
    if (cpuPercent === null) return '—';
    return `${cpuPercent.toFixed(1)}%`;
  };

  const formatUptime = (uptime: string | null) => uptime ?? '—';

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle className="flex items-center gap-2">
            <Server className="h-5 w-5" />
            Cluster Nodes
          </CardTitle>
          <div className="flex items-center gap-2">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
              <Input
                placeholder="Search nodes..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="pl-9 w-64"
              />
            </div>
            <Button variant="outline" size="icon" onClick={onRefresh} disabled={isLoading}>
              <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
            </Button>
          </div>
        </div>
      </CardHeader>
      <CardContent>
        {isLoading && nodes.length === 0 ? (
          <div className="flex items-center justify-center py-8">
            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
          </div>
        ) : (
          <div className="border rounded-lg">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Node</TableHead>
                  <TableHead>Role</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Host</TableHead>
                  <TableHead className="text-right">Memory</TableHead>
                  <TableHead className="text-right">CPU</TableHead>
                  <TableHead>Uptime</TableHead>
                  <TableHead>API Address</TableHead>
                  <TableHead>RPC Address</TableHead>
                  <TableHead className="text-center">Groups</TableHead>
                  <TableHead className="text-right">Term</TableHead>
                  <TableHead className="text-right">Applied Log</TableHead>
                  <TableHead className="text-right">Leader Log</TableHead>
                  <TableHead className="text-right">Snapshot</TableHead>
                  <TableHead className="text-right">Lag</TableHead>
                  <TableHead className="text-center">Progress</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {filteredNodes.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={16} className="text-center py-8 text-muted-foreground">
                      {searchQuery ? 'No nodes match your search' : 'No cluster nodes found'}
                    </TableCell>
                  </TableRow>
                ) : (
                  filteredNodes.map((node) => (
                    <TableRow key={node.node_id}>
                      <TableCell>
                        <div className="flex items-center gap-2">
                          <span className="font-mono font-medium">{node.node_id}</span>
                          {node.is_self && (
                            <TooltipProvider>
                              <Tooltip>
                                <TooltipTrigger>
                                  <CheckCircle2 className="h-4 w-4 text-blue-600" />
                                </TooltipTrigger>
                                <TooltipContent>
                                  <p>Current Node</p>
                                </TooltipContent>
                              </Tooltip>
                            </TooltipProvider>
                          )}
                          {node.is_leader && (
                            <TooltipProvider>
                              <Tooltip>
                                <TooltipTrigger>
                                  <Crown className="h-4 w-4 text-yellow-600" />
                                </TooltipTrigger>
                                <TooltipContent>
                                  <p>Cluster Leader</p>
                                </TooltipContent>
                              </Tooltip>
                            </TooltipProvider>
                          )}
                        </div>
                      </TableCell>
                      <TableCell>{getRoleBadge(node.role)}</TableCell>
                      <TableCell>{getStatusBadge(node.status)}</TableCell>
                      <TableCell className="font-mono text-sm text-muted-foreground">
                        {node.hostname ?? '—'}
                      </TableCell>
                      <TableCell className="text-right font-mono text-sm">
                        {formatMemory(node.memory_usage_mb)}
                      </TableCell>
                      <TableCell className="text-right font-mono text-sm">
                        {formatCpu(node.cpu_usage_percent)}
                      </TableCell>
                      <TableCell className="font-mono text-sm text-muted-foreground">
                        {formatUptime(node.uptime_human)}
                      </TableCell>
                      <TableCell className="font-mono text-sm">{node.api_addr}</TableCell>
                      <TableCell className="font-mono text-sm text-muted-foreground">
                        {node.rpc_addr}
                      </TableCell>
                      <TableCell className="text-center">
                        <span className="text-sm">
                          {node.groups_leading}/{node.total_groups}
                        </span>
                      </TableCell>
                      <TableCell className="text-right font-mono text-sm">
                        {formatNumber(node.current_term)}
                      </TableCell>
                      <TableCell className="text-right font-mono text-sm">
                        {formatNumber(node.last_applied_log)}
                      </TableCell>
                      <TableCell className="text-right font-mono text-sm text-muted-foreground">
                        {formatNumber(node.leader_last_log_index)}
                      </TableCell>
                      <TableCell className="text-right font-mono text-sm text-muted-foreground">
                        {formatNumber(node.snapshot_index)}
                      </TableCell>
                      <TableCell className="text-right font-mono text-sm">
                        {node.replication_lag !== null && node.replication_lag > 0 ? (
                          <span className="text-orange-600">{formatNumber(node.replication_lag)}</span>
                        ) : (
                          <span className="text-muted-foreground">{formatNumber(node.replication_lag)}</span>
                        )}
                      </TableCell>
                      <TableCell className="text-center">
                        {node.catchup_progress_pct !== null ? (
                          <div className="flex items-center gap-2">
                            <div className="flex-1 bg-gray-200 rounded-full h-2">
                              <div
                                className="bg-blue-600 h-2 rounded-full"
                                style={{ width: `${node.catchup_progress_pct}%` }}
                              />
                            </div>
                            <span className="text-xs font-mono w-10 text-right">
                              {node.catchup_progress_pct}%
                            </span>
                          </div>
                        ) : (
                          <span className="text-muted-foreground text-sm">—</span>
                        )}
                      </TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
