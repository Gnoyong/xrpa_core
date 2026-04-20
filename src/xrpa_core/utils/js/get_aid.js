return (function () {
  try {
    if (typeof _accountInfoParam !== 'undefined' && _accountInfoParam?.aid) {
      return String(_accountInfoParam.aid);
    }

    const cookieMatch = document.cookie.match(/(?:^|;\s*)app_id_unified_seller_env=([^;]+)/);
    if (cookieMatch?.[1]) {
      return decodeURIComponent(cookieMatch[1]);
    }

    const localStorageKeys = [
      'app_id_unified_seller_env',
      'seller_aid',
      'aid',
    ];

    for (const key of localStorageKeys) {
      const value = window.localStorage?.getItem(key);
      if (value) {
        return value;
      }
    }
  } catch (error) {
    console.warn('get_aid.js failed', error);
  }

  return null;
})();
