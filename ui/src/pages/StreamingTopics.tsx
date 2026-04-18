import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { RefreshCw, Search, ArrowRight, ArrowUp, ArrowDown, ArrowUpDown } from "lucide-react";
import { PageLayout } from "@/components/layout/PageLayout";
import { StreamingTabs } from "@/features/streaming/components/StreamingTabs";
import { useGetStreamingTopicsQuery } from "@/store/apiSlice";
import { buildTopicSqlSnippet } from "@/features/streaming/sql";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { formatUtcTimestamp } from "@/lib/formatters";

type SortKey = "topicId" | "partitions" | "routeCount" | "retentionSeconds" | "updatedAt";
type SortDirection = "asc" | "desc";

function formatNullableNumber(value: number | null): string {
  if (value === null || value === undefined) {
    return "-";
  }
  return value.toLocaleString();
}

export default function StreamingTopics() {
  const navigate = useNavigate();
  const {
    data: topics = [],
    isFetching,
    error,
    refetch,
  } = useGetStreamingTopicsQuery();
  const [search, setSearch] = useState("");
  const [sortKey, setSortKey] = useState<SortKey>("topicId");
  const [directions, setDirections] = useState<Record<SortKey, SortDirection>>({
    topicId: "desc",
    partitions: "desc",
    routeCount: "desc",
    retentionSeconds: "desc",
    updatedAt: "desc",
  });
  const sortDirection = directions[sortKey];
  const [sortedTopics, setSortedTopics] = useState<typeof topics>([]);

  const filteredTopics = useMemo(() => {
    const query = search.trim().toLowerCase();
    if (!query) {
      return topics;
    }
    return topics.filter(
      (topic) =>
        topic.topicId.toLowerCase().includes(query) ||
        topic.name.toLowerCase().includes(query),
    );
  }, [search, topics]);

  useEffect(() => {
    setSortedTopics(filteredTopics);
  }, [filteredTopics]);

  const applySort = (data: typeof topics, key: SortKey, direction: SortDirection) => {
    const dir = direction === "desc" ? -1 : 1;
    return [...data].sort((a, b) => {
      const aVal = a[key];
      const bVal = b[key];
      if (aVal === null || aVal === undefined) return 1;
      if (bVal === null || bVal === undefined) return -1;
      if (typeof aVal === "string" && typeof bVal === "string") {
        return aVal.localeCompare(bVal) * dir;
      }
      if (typeof aVal === "number" && typeof bVal === "number") {
        return (aVal - bVal) * dir;
      }
      return 0;
    });
  };

  const handleSort = (key: SortKey) => {
    const newDirection = directions[key] === "asc" ? "desc" : "asc";
    setSortKey(key);
    setDirections((prev) => ({ ...prev, [key]: newDirection }));
    setSortedTopics((prev) => applySort(prev, key, newDirection));
  };

  const errorMessage =
    error && "error" in error && typeof error.error === "string"
      ? error.error
      : error
        ? "Failed to load topics"
        : null;

  return (
    <PageLayout
      title="Streaming"
      description="Topic inventory and streaming diagnostics"
      actions={(
        <Button variant="outline" size="sm" onClick={() => void refetch()} disabled={isFetching}>
          <RefreshCw className={`mr-1.5 h-4 w-4 ${isFetching ? "animate-spin" : ""}`} />
          Refresh
        </Button>
      )}
    >
      <StreamingTabs />

      <div className="relative max-w-sm">
        <Search className="pointer-events-none absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
        <Input
          placeholder="Search topics..."
          className="pl-9"
          value={search}
          onChange={(event) => setSearch(event.target.value)}
        />
      </div>

      {errorMessage && (
        <Card className="border-destructive/30 bg-destructive/5">
          <CardContent className="pt-4 text-sm text-destructive">{errorMessage}</CardContent>
        </Card>
      )}

      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-base">Topics</CardTitle>
          <CardDescription>
            {sortedTopics.length} topic{sortedTopics.length === 1 ? "" : "s"} visible
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="rounded-md border">
            <Table>
              <TableHeader>
                <TableRow>
                  {([
                    ["topicId", "Topic"],
                    ["partitions", "Partitions"],
                    ["routeCount", "Routes"],
                    ["retentionSeconds", "Retention (sec)"],
                    ["updatedAt", "Updated"],
                  ] as [SortKey, string][]).map(([key, label]) => (
                    <TableHead
                      key={key}
                      className="cursor-pointer select-none hover:text-foreground"
                      onClick={() => handleSort(key)}
                    >
                      <span className="inline-flex items-center gap-1">
                        {label}
                        {sortKey === key
                          ? (sortDirection === "asc" ? <ArrowUp className="h-3 w-3" /> : <ArrowDown className="h-3 w-3" />)
                          : <ArrowUpDown className="h-3 w-3 opacity-30" />}
                      </span>
                    </TableHead>
                  ))}
                  <TableHead className="w-[170px]">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {sortedTopics.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={6} className="py-8 text-center text-muted-foreground">
                      {search.trim() ? "No topics match your search." : "No topics found."}
                    </TableCell>
                  </TableRow>
                ) : (
                  sortedTopics.map((topic) => (
                    <TableRow key={topic.topicId}>
                      <TableCell className="font-mono text-xs">{topic.topicId}</TableCell>
                      <TableCell>{topic.partitions}</TableCell>
                      <TableCell>{topic.routeCount}</TableCell>
                      <TableCell>{formatNullableNumber(topic.retentionSeconds)}</TableCell>
                      <TableCell className="font-mono text-xs">{formatUtcTimestamp(topic.updatedAt)}</TableCell>
                      <TableCell>
                        <div className="flex items-center gap-2">
                          <Button
                            size="sm"
                            variant="outline"
                            onClick={() => {
                              navigate("/sql", {
                                state: {
                                  prefillSql: buildTopicSqlSnippet(topic.topicId),
                                  prefillTitle: `Topic ${topic.topicId}`,
                                },
                              });
                            }}
                          >
                            Open SQL
                          </Button>
                          <Button
                            size="sm"
                            onClick={() => navigate(`/streaming/topics/${encodeURIComponent(topic.topicId)}`)}
                          >
                            Inspect
                            <ArrowRight className="ml-1 h-3.5 w-3.5" />
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </div>
        </CardContent>
      </Card>
    </PageLayout>
  );
}

