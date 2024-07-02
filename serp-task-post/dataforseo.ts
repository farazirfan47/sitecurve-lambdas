export const createAuthenticatedFetch = () => {

    const username = 'username';
    const password = 'password';

    return (url: RequestInfo, init?: RequestInit): Promise<Response> => {
      const token = btoa(`${username}:${password}`);
      const authHeader = { 'Authorization': `Basic ${token}` };

      const newInit: RequestInit = {
        ...init,
        headers: {
          ...init?.headers,
          ...authHeader
        }
      };

      return fetch(url, newInit);
    };
}