import { RootState } from '@redux';
/**
 * Set local storage with user and token
 */
function syncStateLocalStorage(state: RootState) {
    try {
        const { tokens = {} } = state.auth;
        // const { userData: user = {} } = state.user;
        // set token to local storage
        if (tokens !== undefined) {
            localStorage.setItem('tokens', JSON.stringify(tokens));
        }
        // set user to local storage
        // if (user !== undefined) {
        //     localStorage.setItem('user', JSON.stringify(user));
        // }
        return true;
    } catch (err) {
        return undefined;
    }
}

export default syncStateLocalStorage;
