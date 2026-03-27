import { useState } from "react";
import { Download, ExternalLink, File, FileText, Image, Link2, Music, Video } from "lucide-react";
import { FileRef } from "kalam-link";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { getBackendOrigin } from "@/lib/backend-url";

interface FileDisplayProps {
  value: unknown;
  namespace?: string;
  tableName?: string;
  baseUrl?: string;
}

function getFileIcon(mime: string) {
  if (mime.startsWith("image/")) return <Image className="h-3.5 w-3.5 text-sky-400" />;
  if (mime.startsWith("video/")) return <Video className="h-3.5 w-3.5 text-violet-400" />;
  if (mime.startsWith("audio/")) return <Music className="h-3.5 w-3.5 text-emerald-400" />;
  if (mime.startsWith("text/")) return <FileText className="h-3.5 w-3.5 text-slate-400" />;
  return <File className="h-3.5 w-3.5 text-slate-400" />;
}

export function FileDisplay({ value, namespace, tableName, baseUrl }: FileDisplayProps) {
  const [isOpen, setIsOpen] = useState(false);
  const fileRef = FileRef.from(value);

  if (!fileRef) {
    return <span className="text-muted-foreground italic">null</span>;
  }

  const serverUrl = baseUrl || getBackendOrigin();
  const downloadUrl = namespace && tableName
    ? fileRef.getDownloadUrl(serverUrl, namespace, tableName)
    : null;
  const label = `${fileRef.name} (${fileRef.formatSize()})`;

  return (
    <>
      <button
        type="button"
        onClick={(event) => {
          event.stopPropagation();
          setIsOpen(true);
        }}
        className="inline-flex max-w-full items-center gap-1.5 rounded px-1 py-0.5 text-xs text-sky-500 hover:bg-sky-500/10 hover:text-sky-400"
        title={label}
      >
        <Link2 className="h-3.5 w-3.5 shrink-0" />
        <span className="truncate font-mono">{label}</span>
      </button>

      <Dialog open={isOpen} onOpenChange={setIsOpen}>
        <DialogContent className="max-w-lg">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              {getFileIcon(fileRef.mime)}
              <span className="truncate">{fileRef.name}</span>
            </DialogTitle>
          </DialogHeader>

          <div className="space-y-3 rounded-md border bg-muted/40 p-3 text-xs">
            <div className="grid grid-cols-[90px_1fr] gap-2">
              <span className="text-muted-foreground">Size</span>
              <span className="font-medium">{fileRef.formatSize()}</span>
            </div>
            <div className="grid grid-cols-[90px_1fr] gap-2">
              <span className="text-muted-foreground">Type</span>
              <span className="font-medium">{fileRef.mime}</span>
            </div>
            <div className="grid grid-cols-[90px_1fr] gap-2">
              <span className="text-muted-foreground">ID</span>
              <span className="truncate font-mono">{fileRef.id}</span>
            </div>
          </div>

          {downloadUrl ? (
            <div className="flex items-center gap-2">
              <Button asChild variant="secondary" className="flex-1">
                <a href={downloadUrl} target="_blank" rel="noreferrer" onClick={(event) => event.stopPropagation()}>
                  <ExternalLink className="mr-2 h-4 w-4" />
                  View File
                </a>
              </Button>
              <Button asChild className="flex-1">
                <a href={downloadUrl} download={fileRef.name} onClick={(event) => event.stopPropagation()}>
                  <Download className="mr-2 h-4 w-4" />
                  Download
                </a>
              </Button>
            </div>
          ) : (
            <p className="text-xs text-muted-foreground">
              Open the file from a direct table query so namespace and table context are available for download.
            </p>
          )}
        </DialogContent>
      </Dialog>
    </>
  );
}
