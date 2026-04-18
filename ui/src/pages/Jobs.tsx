import { JobList } from "@/components/jobs/JobList";
import { PageLayout } from "@/components/layout/PageLayout";

export default function Jobs() {
  return (
    <PageLayout
      title="Jobs"
      description="View and monitor background jobs in the system"
      className="flex flex-col h-full min-h-0 gap-6"
      contentClassName="flex-1 min-h-0 flex flex-col gap-4"
    >
      <JobList />
    </PageLayout>
  );
}
