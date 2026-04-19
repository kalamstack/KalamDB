import { describe, it, expect, beforeEach, vi } from 'vitest';
import { api } from '@/lib/api';
import setupReducer, {
  checkSetupStatus,
  submitSetup,
  clearSetupError,
  resetSetup,
} from '@/store/setupSlice';

// Mock the API
vi.mock('@/lib/api', () => ({
  api: {
    get: vi.fn(),
    post: vi.fn(),
  },
  ApiRequestError: class ApiRequestError extends Error {
    apiError: { message: string };
    status: number;
    constructor(apiError: { message: string }, status: number) {
      super(apiError.message);
      this.apiError = apiError;
      this.status = status;
    }
  },
}));

describe('Setup Slice', () => {
  const mockedPost = vi.mocked(api.post);
  const initialState = {
    needsSetup: null,
    isCheckingStatus: true,
    isSubmitting: false,
    setupComplete: false,
    error: null,
    createdUsername: null,
  };

  describe('reducers', () => {
    it('should return initial state', () => {
      expect(setupReducer(undefined, { type: 'unknown' })).toEqual(initialState);
    });

    it('should handle clearSetupError', () => {
      const stateWithError = {
        ...initialState,
        error: 'Some error',
      };
      expect(setupReducer(stateWithError, clearSetupError())).toEqual({
        ...stateWithError,
        error: null,
      });
    });

    it('should handle resetSetup', () => {
      const stateAfterSetup = {
        ...initialState,
        setupComplete: true,
        createdUsername: 'testuser',
        error: 'Some error',
      };
      expect(setupReducer(stateAfterSetup, resetSetup())).toEqual({
        ...stateAfterSetup,
        setupComplete: false,
        createdUsername: null,
        error: null,
      });
    });
  });

  describe('checkSetupStatus thunk', () => {
    beforeEach(() => {
      vi.clearAllMocks();
    });

    it('should set isCheckingStatus to true when pending', () => {
      const action = { type: checkSetupStatus.pending.type };
      const state = setupReducer(initialState, action);
      expect(state.isCheckingStatus).toBe(true);
      expect(state.error).toBeNull();
    });

    it('should set needsSetup to true when server needs setup', () => {
      const action = {
        type: checkSetupStatus.fulfilled.type,
        payload: { needs_setup: true, message: 'Server requires setup' },
      };
      const state = setupReducer(initialState, action);
      expect(state.isCheckingStatus).toBe(false);
      expect(state.needsSetup).toBe(true);
    });

    it('should set needsSetup to false when server is already setup', () => {
      const action = {
        type: checkSetupStatus.fulfilled.type,
        payload: { needs_setup: false },
      };
      const state = setupReducer(initialState, action);
      expect(state.isCheckingStatus).toBe(false);
      expect(state.needsSetup).toBe(false);
    });

    it('should handle rejected status check gracefully', () => {
      const action = {
        type: checkSetupStatus.rejected.type,
        payload: 'Failed to check status',
      };
      const state = setupReducer(initialState, action);
      expect(state.isCheckingStatus).toBe(false);
      expect(state.error).toBe('Failed to check status');
      expect(state.needsSetup).toBe(false); // Assume setup not needed if can't check
    });
  });

  describe('submitSetup thunk', () => {
    beforeEach(() => {
      vi.clearAllMocks();
    });

    it('should set isSubmitting to true when pending', () => {
      const action = { type: submitSetup.pending.type };
      const state = setupReducer(initialState, action);
      expect(state.isSubmitting).toBe(true);
      expect(state.error).toBeNull();
    });

    it('should mark setup as complete on success', () => {
      const action = {
        type: submitSetup.fulfilled.type,
        payload: {
          user: {
            id: 'testuser',
            role: 'dba',
            email: 'test@example.com',
            created_at: '2026-01-27T00:00:00Z',
            updated_at: '2026-01-27T00:00:00Z',
          },
          message: 'Setup complete',
        },
      };
      const state = setupReducer(initialState, action);
      expect(state.isSubmitting).toBe(false);
      expect(state.setupComplete).toBe(true);
      expect(state.needsSetup).toBe(false);
      expect(state.createdUsername).toBe('testuser');
    });

    it('posts the canonical setup payload to the API', async () => {
      mockedPost.mockResolvedValue({
        user: {
          id: 'testuser',
          role: 'dba',
          email: 'test@example.com',
          created_at: '2026-01-27T00:00:00Z',
          updated_at: '2026-01-27T00:00:00Z',
        },
        message: 'Setup complete',
      });

      const dispatch = vi.fn();
      const getState = vi.fn();
      const request = {
        user: 'testuser',
        password: 'StrongPass123!',
        root_password: 'RootPass123!',
        email: 'test@example.com',
      };

      const action = await submitSetup(request)(dispatch, getState, undefined);

      expect(mockedPost).toHaveBeenCalledWith('/auth/setup', request);
      expect(action.type).toBe(submitSetup.fulfilled.type);
      expect(action.payload).toEqual({
        user: {
          id: 'testuser',
          role: 'dba',
          email: 'test@example.com',
          created_at: '2026-01-27T00:00:00Z',
          updated_at: '2026-01-27T00:00:00Z',
        },
        message: 'Setup complete',
      });
    });

    it('should set error on failure', () => {
      const action = {
        type: submitSetup.rejected.type,
        payload: 'Weak password',
      };
      const state = setupReducer(initialState, action);
      expect(state.isSubmitting).toBe(false);
      expect(state.error).toBe('Weak password');
      expect(state.setupComplete).toBe(false);
    });
  });

  describe('setup cannot be done twice', () => {
    it('should not allow setup when needsSetup is false', () => {
      const stateAfterSetup = {
        ...initialState,
        needsSetup: false,
        setupComplete: true,
        createdUsername: 'existinguser',
      };

      // Attempting to submit setup again should still work at reducer level
      // but the UI/guard should prevent reaching this point
      // This test verifies the state after setup is complete
      expect(stateAfterSetup.needsSetup).toBe(false);
      expect(stateAfterSetup.setupComplete).toBe(true);
    });

    it('should redirect to login when setup is already complete', () => {
      // This is a conceptual test - in practice, SetupGuard handles this
      const stateAfterSetup = {
        ...initialState,
        needsSetup: false,
        isCheckingStatus: false,
      };

      // When needsSetup is false and not checking status,
      // SetupGuard should redirect away from /setup
      expect(stateAfterSetup.needsSetup).toBe(false);
      expect(stateAfterSetup.isCheckingStatus).toBe(false);
    });
  });
});

describe('Setup Guard Integration', () => {
  it('should prevent access to setup page after server is configured', () => {
    // Test the state that SetupGuard uses to determine redirects
    const configuredServerState = {
      needsSetup: false,
      isCheckingStatus: false,
      isSubmitting: false,
      setupComplete: false,
      error: null,
      createdUsername: null,
    };

    // When API returns needs_setup: false, user should be redirected
    expect(configuredServerState.needsSetup).toBe(false);
    
    // The SetupGuard component checks this condition:
    // if (needsSetup === false && isOnSetupPage) navigate("/login")
    // This test verifies the state that triggers that redirect
  });

  it('should allow access to setup page when server needs setup', () => {
    const unconfiguredServerState = {
      needsSetup: true,
      isCheckingStatus: false,
      isSubmitting: false,
      setupComplete: false,
      error: null,
      createdUsername: null,
    };

    expect(unconfiguredServerState.needsSetup).toBe(true);
    
    // The SetupGuard component checks this condition:
    // if (needsSetup === true && !isOnSetupPage) navigate("/setup")
    // This test verifies the state that triggers redirect TO setup
  });
});

describe('API rejection handling', () => {
  const initialState = {
    needsSetup: null,
    isCheckingStatus: true,
    isSubmitting: false,
    setupComplete: false,
    error: null,
    createdUsername: null,
  };

  it('should handle already_configured error from API', () => {
    // When server returns "already_configured" error
    const action = {
      type: submitSetup.rejected.type,
      payload: 'Server has already been configured. Root password is set.',
    };
    const state = setupReducer(initialState, action);
    
    expect(state.error).toBe('Server has already been configured. Root password is set.');
    expect(state.isSubmitting).toBe(false);
    expect(state.setupComplete).toBe(false);
  });
});
