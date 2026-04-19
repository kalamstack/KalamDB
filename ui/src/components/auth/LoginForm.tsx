import { useState } from "react";
import { Link } from "react-router-dom";
import { AlertCircle, KeyRound, User } from "lucide-react";
import { useAuth } from "@/lib/auth";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

interface LoginFormProps {
  onSuccess?: () => void;
}

export default function LoginForm({ onSuccess }: LoginFormProps) {
  const { login, error, isLoading } = useAuth();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [localError, setLocalError] = useState<string | null>(null);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLocalError(null);

    if (!username || !password) {
      setLocalError("Please enter username and password");
      return;
    }

    try {
      await login({ user: username, password });
      onSuccess?.();
    } catch {
      // Error is already handled by auth context
    }
  };

  const displayError = localError || error;

  return (
    <form onSubmit={handleSubmit} className="space-y-4">
      <Button type="button" variant="secondary" className="h-11 w-full" disabled>
        Continue with Google (Soon)
      </Button>

      <div className="flex items-center gap-3 py-2">
        <div className="h-px flex-1 bg-border" />
        <span className="text-xs font-medium text-muted-foreground">OR</span>
        <div className="h-px flex-1 bg-border" />
      </div>

      {displayError && (
        <Alert variant="destructive">
          <AlertCircle className="h-4 w-4" />
          <AlertDescription>{displayError}</AlertDescription>
        </Alert>
      )}

      <div className="space-y-3">
        <div className="space-y-2">
          <label htmlFor="username" className="text-sm font-medium">
            Username
          </label>
          <div className="relative">
            <User className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              id="username"
              type="text"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              placeholder="Enter username"
              disabled={isLoading}
              autoComplete="username"
              autoFocus
              className="h-11 pl-10"
            />
          </div>
        </div>

        <div className="space-y-2">
          <label htmlFor="password" className="text-sm font-medium">
            Password
          </label>
          <div className="relative">
            <KeyRound className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              id="password"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="Enter password"
              disabled={isLoading}
              autoComplete="current-password"
              className="h-11 pl-10"
            />
          </div>
        </div>
      </div>

      <Button type="submit" className="h-11 w-full" disabled={isLoading}>
        {isLoading ? "Signing in..." : "Log in"}
      </Button>

      <div className="space-y-1 text-center text-sm text-muted-foreground">
        <p>
          Need setup on an unconfigured node?{" "}
          <Link to="/setup" className="font-medium text-primary hover:underline">
            Run setup
          </Link>
        </p>
        <p className="text-xs">
          Nodes started with scripts/cluster.sh are already configured. Sign in as root with the configured root password.
        </p>
      </div>
    </form>
  );
}
