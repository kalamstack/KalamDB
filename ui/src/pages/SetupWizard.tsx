import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { AlertCircle, Check, Eye, EyeOff, Loader2 } from "lucide-react";
import AuthSplitLayout from "@/components/auth/AuthSplitLayout";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useAppDispatch, useAppSelector } from "@/store/hooks";
import { clearSetupError, submitSetup } from "@/store/setupSlice";

interface FormData {
  username: string;
  password: string;
  confirmPassword: string;
  rootPassword: string;
  confirmRootPassword: string;
  email: string;
}

interface FormErrors {
  username?: string;
  password?: string;
  confirmPassword?: string;
  rootPassword?: string;
  confirmRootPassword?: string;
  email?: string;
}

const STEP_TITLES: Record<1 | 2 | 3, string> = {
  1: "Create DBA account",
  2: "Set DBA password",
  3: "Set root password",
};

const STEP_DESCRIPTIONS: Record<1 | 2 | 3, string> = {
  1: "Create the first administrator account for this server.",
  2: "Choose a secure password for your DBA account.",
  3: "Set the root password used for system-level administration.",
};

export default function SetupWizard() {
  const dispatch = useAppDispatch();
  const navigate = useNavigate();
  const { isSubmitting, setupComplete, error, createdUsername } = useAppSelector((state) => state.setup);

  const [formData, setFormData] = useState<FormData>({
    username: "",
    password: "",
    confirmPassword: "",
    rootPassword: "",
    confirmRootPassword: "",
    email: "",
  });
  const [formErrors, setFormErrors] = useState<FormErrors>({});
  const [showPassword, setShowPassword] = useState(false);
  const [showRootPassword, setShowRootPassword] = useState(false);
  const [step, setStep] = useState<1 | 2 | 3>(1);

  useEffect(() => {
    if (error) {
      dispatch(clearSetupError());
    }
  }, [error, formData, dispatch]);

  useEffect(() => {
    if (!setupComplete) {
      return;
    }
    const timer = setTimeout(() => {
      navigate("/login", { replace: true });
    }, 3000);
    return () => clearTimeout(timer);
  }, [setupComplete, navigate]);

  const validateStep1 = (): boolean => {
    const errors: FormErrors = {};

    if (!formData.username.trim()) {
      errors.username = "Username is required";
    } else if (formData.username.toLowerCase() === "root") {
      errors.username = "Cannot use 'root' as username";
    } else if (formData.username.length < 3) {
      errors.username = "Username must be at least 3 characters";
    }

    if (formData.email && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(formData.email)) {
      errors.email = "Invalid email format";
    }

    setFormErrors(errors);
    return Object.keys(errors).length === 0;
  };

  const validateStep2 = (): boolean => {
    const errors: FormErrors = {};

    if (!formData.password) {
      errors.password = "Password is required";
    } else if (formData.password.length < 8) {
      errors.password = "Password must be at least 8 characters";
    }

    if (formData.password !== formData.confirmPassword) {
      errors.confirmPassword = "Passwords do not match";
    }

    setFormErrors(errors);
    return Object.keys(errors).length === 0;
  };

  const validateStep3 = (): boolean => {
    const errors: FormErrors = {};

    if (!formData.rootPassword) {
      errors.rootPassword = "Root password is required";
    } else if (formData.rootPassword.length < 8) {
      errors.rootPassword = "Root password must be at least 8 characters";
    }

    if (formData.rootPassword !== formData.confirmRootPassword) {
      errors.confirmRootPassword = "Root passwords do not match";
    }

    setFormErrors(errors);
    return Object.keys(errors).length === 0;
  };

  const handleChange = (field: keyof FormData) => (event: React.ChangeEvent<HTMLInputElement>) => {
    const value = event.target.value;
    setFormData((previous) => ({ ...previous, [field]: value }));
    if (formErrors[field]) {
      setFormErrors((previous) => ({ ...previous, [field]: undefined }));
    }
  };

  const handleBack = () => {
    setStep((previous) => (previous === 1 ? previous : ((previous - 1) as 1 | 2 | 3)));
  };

  const handleNext = () => {
    if (step === 1 && validateStep1()) {
      setStep(2);
      return;
    }
    if (step === 2 && validateStep2()) {
      setStep(3);
    }
  };

  const handleSubmit = (event: React.FormEvent) => {
    event.preventDefault();
    if (!validateStep3()) {
      return;
    }

    dispatch(
      submitSetup({
        user: formData.username,
        password: formData.password,
        root_password: formData.rootPassword,
        email: formData.email || undefined,
      }),
    );
  };

  if (setupComplete) {
    return (
      <AuthSplitLayout
        title="Setup complete"
        description="Your server is configured and ready for login."
        panelTitle="Server ready"
        panelDescription="Core admin credentials are configured. Continue to login and start managing your workspace."
        panelFootnote="Initialization"
      >
        <div className="space-y-5">
          <div className="mx-auto flex h-14 w-14 items-center justify-center rounded-full bg-primary/10">
            <Check className="h-7 w-7 text-primary" />
          </div>

          <div className="rounded-lg border bg-muted/50 p-4">
            <p className="text-sm text-muted-foreground">
              DBA account <span className="font-semibold text-foreground">{createdUsername}</span> created successfully.
            </p>
            <p className="mt-2 text-sm text-muted-foreground">Root password has been configured.</p>
          </div>

          <p className="text-sm text-muted-foreground">Redirecting to login in 3 seconds.</p>
          <Button onClick={() => navigate("/login", { replace: true })} className="h-11 w-full">
            Go to login
          </Button>
        </div>
      </AuthSplitLayout>
    );
  }

  return (
    <AuthSplitLayout
      title={STEP_TITLES[step]}
      description={STEP_DESCRIPTIONS[step]}
      panelTitle="First-run configuration"
      panelDescription="Create your initial DBA account and secure root access before opening the admin interface."
      panelFootnote="Setup wizard"
    >
      <form onSubmit={step === 3 ? handleSubmit : (event) => { event.preventDefault(); handleNext(); }} className="space-y-6">
        <div className="space-y-2">
          <div className="flex items-center justify-between text-xs text-muted-foreground">
            <span>Step {step} of 3</span>
            <span>{STEP_TITLES[step]}</span>
          </div>
          <div className="grid grid-cols-3 gap-2">
            {[1, 2, 3].map((value) => (
              <div
                key={value}
                className={`h-1.5 rounded-full ${value <= step ? "bg-primary" : "bg-muted"}`}
              />
            ))}
          </div>
        </div>

        {step === 1 && (
          <div className="space-y-4">
            <div className="space-y-2">
              <label htmlFor="username" className="text-sm font-medium">
                Username <span className="text-destructive">*</span>
              </label>
              <Input
                id="username"
                value={formData.username}
                onChange={handleChange("username")}
                placeholder="admin"
                autoFocus
                className={formErrors.username ? "border-destructive" : ""}
              />
              {formErrors.username && <p className="text-xs text-destructive">{formErrors.username}</p>}
            </div>

            <div className="space-y-2">
              <label htmlFor="email" className="text-sm font-medium">
                Email <span className="text-muted-foreground">(optional)</span>
              </label>
              <Input
                id="email"
                type="email"
                value={formData.email}
                onChange={handleChange("email")}
                placeholder="admin@example.com"
                className={formErrors.email ? "border-destructive" : ""}
              />
              {formErrors.email && <p className="text-xs text-destructive">{formErrors.email}</p>}
            </div>
          </div>
        )}

        {step === 2 && (
          <div className="space-y-4">
            <div className="space-y-2">
              <label htmlFor="password" className="text-sm font-medium">
                Password <span className="text-destructive">*</span>
              </label>
              <div className="relative">
                <Input
                  id="password"
                  type={showPassword ? "text" : "password"}
                  value={formData.password}
                  onChange={handleChange("password")}
                  placeholder="Enter password"
                  autoFocus
                  className={`pr-10 ${formErrors.password ? "border-destructive" : ""}`}
                />
                <button
                  type="button"
                  onClick={() => setShowPassword((previous) => !previous)}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
                  aria-label={showPassword ? "Hide password" : "Show password"}
                >
                  {showPassword ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
                </button>
              </div>
              {formErrors.password && <p className="text-xs text-destructive">{formErrors.password}</p>}
            </div>

            <div className="space-y-2">
              <label htmlFor="confirmPassword" className="text-sm font-medium">
                Confirm Password <span className="text-destructive">*</span>
              </label>
              <Input
                id="confirmPassword"
                type={showPassword ? "text" : "password"}
                value={formData.confirmPassword}
                onChange={handleChange("confirmPassword")}
                placeholder="Confirm password"
                className={formErrors.confirmPassword ? "border-destructive" : ""}
              />
              {formErrors.confirmPassword && <p className="text-xs text-destructive">{formErrors.confirmPassword}</p>}
            </div>
          </div>
        )}

        {step === 3 && (
          <div className="space-y-4">
            <Alert>
              <AlertCircle className="h-4 w-4" />
              <AlertTitle>Security notice</AlertTitle>
              <AlertDescription>
                The root password is used for system administration. Store it securely.
              </AlertDescription>
            </Alert>

            <div className="space-y-2">
              <label htmlFor="rootPassword" className="text-sm font-medium">
                Root Password <span className="text-destructive">*</span>
              </label>
              <div className="relative">
                <Input
                  id="rootPassword"
                  type={showRootPassword ? "text" : "password"}
                  value={formData.rootPassword}
                  onChange={handleChange("rootPassword")}
                  placeholder="Enter root password"
                  autoFocus
                  className={`pr-10 ${formErrors.rootPassword ? "border-destructive" : ""}`}
                />
                <button
                  type="button"
                  onClick={() => setShowRootPassword((previous) => !previous)}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
                  aria-label={showRootPassword ? "Hide root password" : "Show root password"}
                >
                  {showRootPassword ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
                </button>
              </div>
              {formErrors.rootPassword && <p className="text-xs text-destructive">{formErrors.rootPassword}</p>}
            </div>

            <div className="space-y-2">
              <label htmlFor="confirmRootPassword" className="text-sm font-medium">
                Confirm Root Password <span className="text-destructive">*</span>
              </label>
              <Input
                id="confirmRootPassword"
                type={showRootPassword ? "text" : "password"}
                value={formData.confirmRootPassword}
                onChange={handleChange("confirmRootPassword")}
                placeholder="Confirm root password"
                className={formErrors.confirmRootPassword ? "border-destructive" : ""}
              />
              {formErrors.confirmRootPassword && (
                <p className="text-xs text-destructive">{formErrors.confirmRootPassword}</p>
              )}
            </div>
          </div>
        )}

        {error && (
          <Alert variant="destructive">
            <AlertCircle className="h-4 w-4" />
            <AlertTitle>Setup failed</AlertTitle>
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}

        <div className="flex gap-3">
          {step > 1 && (
            <Button type="button" variant="outline" className="flex-1" onClick={handleBack}>
              Back
            </Button>
          )}
          {step < 3 ? (
            <Button type="submit" className="flex-1">
              Continue
            </Button>
          ) : (
            <Button type="submit" className="flex-1" disabled={isSubmitting}>
              {isSubmitting ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Setting up...
                </>
              ) : (
                "Complete setup"
              )}
            </Button>
          )}
        </div>
      </form>
    </AuthSplitLayout>
  );
}
