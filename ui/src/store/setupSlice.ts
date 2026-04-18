import { createSlice, createAsyncThunk } from "@reduxjs/toolkit";
import { api, ApiRequestError } from "../lib/api";

// Types
export interface SetupStatusResponse {
  needs_setup: boolean;
  message?: string;
}

export interface SetupRequest {
  user: string;
  password: string;
  root_password: string;
  email?: string;
}

export interface SetupResponse {
  user: {
    id: string;
    role: string;
    email: string | null;
    created_at: string;
    updated_at: string;
  };
  message: string;
}

interface SetupState {
  needsSetup: boolean | null; // null = unknown, true = needs setup, false = already setup
  isCheckingStatus: boolean;
  isSubmitting: boolean;
  setupComplete: boolean;
  error: string | null;
  createdUsername: string | null;
}

const initialState: SetupState = {
  needsSetup: null,
  isCheckingStatus: true,
  isSubmitting: false,
  setupComplete: false,
  error: null,
  createdUsername: null,
};

// Async thunks
export const checkSetupStatus = createAsyncThunk(
  "setup/checkStatus",
  async (_, { rejectWithValue }) => {
    try {
      const response = await api.get<SetupStatusResponse>("/auth/status");
      return response;
    } catch (err) {
      if (err instanceof ApiRequestError) {
        return rejectWithValue(err.apiError.message);
      }
      return rejectWithValue("Failed to check setup status");
    }
  }
);

export const submitSetup = createAsyncThunk(
  "setup/submit",
  async (request: SetupRequest, { rejectWithValue }) => {
    try {
      const response = await api.post<SetupResponse>("/auth/setup", request);
      return response;
    } catch (err) {
      if (err instanceof ApiRequestError) {
        return rejectWithValue(err.apiError.message);
      }
      return rejectWithValue("Setup failed");
    }
  }
);

const setupSlice = createSlice({
  name: "setup",
  initialState,
  reducers: {
    clearSetupError: (state) => {
      state.error = null;
    },
    resetSetup: (state) => {
      state.setupComplete = false;
      state.createdUsername = null;
      state.error = null;
    },
  },
  extraReducers: (builder) => {
    builder
      // Check Status
      .addCase(checkSetupStatus.pending, (state) => {
        state.isCheckingStatus = true;
        state.error = null;
      })
      .addCase(checkSetupStatus.fulfilled, (state, action) => {
        state.isCheckingStatus = false;
        state.needsSetup = action.payload.needs_setup;
      })
      .addCase(checkSetupStatus.rejected, (state, action) => {
        state.isCheckingStatus = false;
        state.error = action.payload as string;
        // If we can't check status, assume setup is not needed (server might be down)
        state.needsSetup = false;
      })
      // Submit Setup
      .addCase(submitSetup.pending, (state) => {
        state.isSubmitting = true;
        state.error = null;
      })
      .addCase(submitSetup.fulfilled, (state, action) => {
        state.isSubmitting = false;
        state.setupComplete = true;
        state.needsSetup = false;
        state.createdUsername = action.payload.user.id;
      })
      .addCase(submitSetup.rejected, (state, action) => {
        state.isSubmitting = false;
        state.error = action.payload as string;
      });
  },
});

export const { clearSetupError, resetSetup } = setupSlice.actions;
export default setupSlice.reducer;
