import { useParams, useNavigate } from "react-router-dom";
import { AuditLogList } from "@/components/audit/AuditLogList";
import { JobList } from "@/components/jobs/JobList";
import { ServerLogList } from "@/components/logs/ServerLogList";
import { cn } from "@/lib/utils";

type LogTab = "audit" | "jobs" | "server";

const tabs: { id: LogTab; label: string; description: string }[] = [
  { id: "audit", label: "Audit Logs", description: "View system audit trail and activity history" },
  { id: "jobs", label: "Jobs", description: "View and monitor background jobs in the system" },
  { id: "server", label: "Server Logs", description: "View real-time server logs and debug information" },
];

export default function Logging() {
  const { tab } = useParams<{ tab?: string }>();
  const navigate = useNavigate();
  
  // Default to audit logs if no tab specified
  const activeTab = (tab as LogTab) || "audit";

  const handleTabChange = (tabId: LogTab) => {
    navigate(`/logging/${tabId}`);
  };

  const renderContent = () => {
    switch (activeTab) {
      case "audit":
        return <AuditLogList />;
      case "jobs":
        return <JobList />;
      case "server":
        return <ServerLogList />;
      default:
        return <AuditLogList />;
    }
  };

  const activeTabData = tabs.find((t) => t.id === activeTab) || tabs[0];

  return (
    <div className="flex flex-col h-full">
      {/* Tab Navigation */}
      <div className="border-b px-4 lg:px-6 pt-4">
        <div className="mb-4">
          <h1 className="text-2xl font-semibold tracking-tight">Logging</h1>
          <p className="text-sm text-muted-foreground">{activeTabData.description}</p>
        </div>
        <nav className="flex gap-4">
          {tabs.map((tabItem) => (
            <button
              key={tabItem.id}
              onClick={() => handleTabChange(tabItem.id)}
              className={cn(
                "border-b-2 px-4 py-2 text-sm font-medium transition-colors",
                activeTab === tabItem.id
                  ? "border-primary text-primary"
                  : "border-transparent text-muted-foreground hover:text-foreground hover:border-muted-foreground"
              )}
            >
              {tabItem.label}
            </button>
          ))}
        </nav>
      </div>

      {/* Tab Content */}
      <div className="flex-1 min-h-0 p-4 lg:p-6">
        {renderContent()}
      </div>
    </div>
  );
}
