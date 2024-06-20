import argparse
import random
import time

from PIL import Image


import traceback
import sys


def MyExceptionLogger(err, file):
    # return
    traceback_details = traceback.format_exc()
    print(traceback_details, file=file)
    exc_type, exc_value, exc_traceback = sys.exc_info()
    print(f"Exception type: {exc_type}, Value: {exc_value}", file=file)
    traceback.print_tb(exc_traceback, file=file)


try:
    from .utility.utilityMethods import *
    from .config import *
    from . import cmpdetection as cd
except ImportError as E:
    print("run the module as a script")
    from utility.utilityMethods import *
    from config import *
    import cmpdetection as cd

rej_flag = False


def reset():
    global counter, driver, visit_db, domains, this_domain, this_url, banner_db, html_db, this_lang, this_banner_lang, this_run_url
    counter = 0
    driver = None
    visit_db = None
    banner_db = None
    html_db = None
    domains = []
    this_domain = None
    this_url = None
    this_run_url = None
    this_lang = None
    this_banner_lang = None


def run_webdriver_old(page_load_timeout=TIME_OUT, profile=None):
    global driver, HEADLESS
    options = Options()
    driver_path = "./geckodriver.exe"
    if profile is None:
        # path = '/mnt/c/Users/arasaii/AppData/Roaming/Mozilla/Firefox/Profiles/xob6l1yb.cookies'
        path = r'C:\Users\arasaii\AppData\Roaming\Mozilla\Firefox\Profiles\xob6l1yb.cookies'
        if not os.path.isdir(path):
            # path = r'/mnt/c/Users/arasaii/AppData/Roaming/Mozilla/Firefox/Profiles/xob6l1yb.cookies'
            driver_path = "./geckodriver"
            # options.binary_location = r'/mnt/c/Program Files/Mozilla Firefox/firefox.exe'
            # check when the system is not my own system and therefore does not have this firefox profile.
            if not os.path.isdir(path):
                path = None
                HEADLESS = True
            # path = None
        # profile = webdriver.FirefoxProfile(
        #     path)
        options.add_argument(path)
    # desired, prof = avoid_bot_detection(profile, MOBILE_AGENT)
    # options.set_preference("browser.privatebrowsing.autostart", True)
    # options.add_argument("--incognito")
    options.headless = HEADLESS
    options.add_argument("-profile")

    # prefs = { "translate_whitelists": {"fr": "en", "it": "en"}, "translate": {"enabled": "true"}}
    # options.add_experimental_option("prefs", prefs)
    d = DesiredCapabilities.FIREFOX
    d['loggingPrefs'] = {'browser': 'ALL'}
    try:
        driver = webdriver.Firefox(options=options)
    except WebDriverException as Ex:
        print("Error while run webdriver: ", Ex.__str__())

    driver.set_page_load_timeout(page_load_timeout)
    if MOBILE_AGENT:
        driver.set_window_size(340, 695)
    else:
        driver.maximize_window()
    # Must be the full path to an XPI file!
    never_consent_extension_win_path = r'C:\Drives\Education\MPI\Intern\Codes\Workstation\bannerdetection\neverconsent\N1.xpi'
    # id = driver.install_addon(never_consent_extension_win_path, temporary=True)
    # translate_extension_path = r'C:\Users\TwilighT\AppData\Roaming\Mozilla\Firefox\Profiles\24jg4ggm.default-release\extensions\jid1-93WyvpgvxzGATw@jetpack.xpi'  # Must be the full path to an XPI file!
    # driver.install_addon(translate_extension_path, temporary=True)

    return driver


def run_webdriver(page_load_timeout=30, profile=None):
    global HEADLESS
    options = Options()
    driver_path = "./geckodriver.exe"
    # binary_location = r'C:\Program Files\Mozilla Firefox\firefox.exe'
    binary_location = r'C:\Program Files\Firefox Nightly\firefox.exe'
    if profile is None:
        profile_path = r'C:\Users\arasaii\AppData\Roaming\Mozilla\Firefox\Profiles\xob6l1yb.cookies'
        if not os.path.isdir(profile_path):
            profile_path = r'/mnt/c/Users/arasaii/AppData/Roaming/Mozilla/Firefox/Profiles/xob6l1yb.cookies'
            driver_path = "./geckodriver"
            # check when the system is not my own system and therefore does not have this firefox profile.
            if not os.path.isdir(profile_path):
                profile_path = None
                HEADLESS = True
            else:
                binary_location = r'/mnt/c/Program Files/Mozilla Firefox/firefox.exe'
                options.binary_location = binary_location
    options.set_preference("browser.privatebrowsing.autostart", True)
    options.add_argument("--incognito")
    if HEADLESS:
        options.add_argument('-headless')

    # options.set_preference('profile', profile_path)
    # options.add_argument("-profile")
    # options.add_argument(profile_path)
    service = Service(driver_path)
    driver = webdriver.Firefox(options=options, service=service)
    driver.set_page_load_timeout(page_load_timeout)
    driver.maximize_window()
    return driver

    try:
        driver = webdriver.Firefox(options=options, service=service)
    except WebDriverException as Ex:
        print("Error while run webdriver: ", Ex.__str__())

    driver.set_page_load_timeout(page_load_timeout)
    if MOBILE_AGENT:
        driver.set_window_size(340, 695)
    else:
        driver.maximize_window()
    return driver


def run_webdriver(page_load_timeout=30, profile=None):
    global HEADLESS, STATELESS
    options = Options()
    driver_path = "./geckodriver.exe"
    # binary_location = r'C:\Program Files\Mozilla Firefox\firefox.exe'

    if profile is None:
        # C:\Users\arasaii\AppData\Local\Mozilla\Firefox\Profiles  m1rl46nh.StateLess 8tptx6pp.crawls
        if STATELESS:
            profile_path = r'C:\Users\arasaii\AppData\Roaming\Mozilla\Firefox\Profiles\m1rl46nh.StateLess'
        else:
            profile_path = r'C:\Users\arasaii\AppData\Roaming\Mozilla\Firefox\Profiles\7h1edsai.StateFull'
        # profile_path = r'C:\Users\arasaii\AppData\Roaming\Mozilla\Firefox\Profiles\wvskwjwm.default-beta-1'

        if not os.path.isdir(profile_path):
            profile_path = r'/mnt/c/Users/arasaii/AppData/Roaming/Mozilla/Firefox/Profiles/m1rl46nh.StateLess'
            # check when the system is not my own system and therefore does not have this firefox profile.
            if not os.path.isdir(profile_path):
                profile_path = None
                HEADLESS = True
            else:
                binary_location = r'/mnt/c/Program Files/Mozilla Firefox/firefox.exe'
                # binary_location = r'/usr/bin/firefox'
                options.binary_location = binary_location
                driver_path = "./geckodriver"
        else:
            binary_location = r'C:\Program Files\Mozilla Firefox\firefox.exe'
            options.binary_location = binary_location
            # options.add_argument(profile_path)
    options.set_preference("browser.privatebrowsing.autostart", True)
    options.add_argument("--incognito")
    if HEADLESS:
        options.add_argument('-headless')
    options.set_preference('profile', profile_path)
    options.add_argument("-profile")
    options.add_argument(profile_path)

    # profile = webdriver.FirefoxProfile(profile_path)

    # service = Service(driver_path)
    try:
        driver = webdriver.Firefox(options=options)
    except WebDriverException as Ex:
        print("Error while run webdriver: ", Ex.__str__())
        raise
    driver.set_page_load_timeout(page_load_timeout)
    driver.maximize_window()
    if UBLOCK_ADDON:
        install_ublock(driver)

    return driver


def run_chrome(page_load_timeout=TIME_OUT):
    global driver
    options = webdriver.ChromeOptions()
    # options.add_argument('--headless')
    # options.set_binary("C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe")
    # options.binary_location = "/mnt/c/Program Files/Google/Chrome/Application/chrome.exe"
    # options.add_argument("/mnt/c/Users/arasaii/AppData/Local/Google/Chrome/User Data/Default")
    driver = webdriver.Chrome(
        executable_path="./chromedriver.exe", options=options)
    driver.set_page_load_timeout(page_load_timeout)
    driver.maximize_window()
    shadow_dom_opening = """Element.prototype._attachShadow = Element.prototype.attachShadow;
Element.prototype.attachShadow = function () {
    return this._attachShadow( { mode: "open" } );
};"""
    # driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {'source': shadow_dom_opening})
    return driver


def set_webdriver(web_driver=None):
    global driver
    try:
        if web_driver is None:
            web_driver = run_webdriver()
        if driver != webdriver:
            driver = web_driver
            if MOBILE_AGENT:
                driver.set_window_size(340, 695)
            else:
                driver.maximize_window()

        # TODO: Added by ME for Cookie SRC Saver
        # COOKIESRC_SAVE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'CookieSrcSaver/dist')
        # driver.install_addon(path=COOKIESRC_SAVE_PATH, temporary=True)

        if UBLOCK_ADDON:
            install_ublock(driver)
    except Exception as ex:
        with open(log_file, 'a+') as f:
            print(f"failed in bc.set_webdriver: f{ex.__str__()}", file=f)
            MyExceptionLogger(err=ex, file=f)
    return driver


def install_ublock(web_driver):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    ublock_xpi_path = current_dir + "/ublock/uBlock0@raymondhill.net.xpi"
    web_driver.install_addon(ublock_xpi_path)


def get_data_dir_name():
    global data_dir
    return data_dir


def set_data_dir_name(dir_name):
    global data_dir
    data_dir = dir_name


def init(headless=HEADLESS, input_file=None, num_browsers=NUM_BROWSERS, num_repetitions=1, domains_file=None, web_driver=None, v_db=None,
         b_db=None, h_db=None):  # initialize bannerdetection by setting url file and webdriver instance
    global domains, driver, file, input_files_dir, UBLOCK_ADDON
    url_dir = "." + input_files_dir
    if web_driver is None:
        if CHROME:
            driver = run_chrome()
        else:
            driver = run_webdriver()
    else:
        driver = web_driver
    if domains_file is None:
        file = url_dir+urls_file
    else:
        file = url_dir+domains_file
    create_data_dirs()
    if os.path.isfile(file):
        domains = file_to_list(file)
    # set_database(v_db, b_db, h_db)

    if input_file:
        file = input_file
    init_str = f"""Crawl initialized for: {file} in {datetime.now().strftime("%H-%M-%S").__str__()}
    START_POINT:STEP_SIZE: {START_POINT}:{STEP_SIZE}
    headless: {headless}
    input_file: {input_file}
    num_browsers: {num_browsers}
    num_repetitions: {num_repetitions}
    timeout: {TIME_OUT}
    translation: {TRANSLATION}
    delay_time: {SLEEP_TIME}
    ATTEMPTS:ATTEMPT_STEP: {ATTEMPTS}:{ATTEMPT_STEP}
    Chrome: {CHROME}
    openwpm.xpi: {XPI}
    Watchdog: {WATCHDOG}
    interaction choice: {"ALL"}
    non explicit: {NON_EXPLICIT}
    SIMPLE_DETECTION: {SIMPLE_DETECTION}
    search for reject btn in setting: {REJ_IN_SET}
    NC_ADDON: {NC_ADDON}
    mobile agent: {MOBILE_AGENT}
    CMP detection: {CMPDETECTION}
    banner interaction: {BANNERINTERACTION} \n\n""" + "__"*30 + "\n"
    print(init_str)

    try:
        with open(log_file, 'a+') as f:
            print(init_str, file=f)
    except:
        pass


def get_domains():
    global domains
    return domains


def create_data_dirs():
    if not os.path.exists(season_dir):
        os.makedirs(season_dir)
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    if not os.path.exists(sc_dir):
        os.makedirs(sc_dir)
    if not os.path.exists(nobanner_sc_dir):
        os.makedirs(nobanner_sc_dir)


def file_to_list(path):
    file = set_urls_file(path)
    global domains
    while True:
        domain = file.readline().strip('\n')
        if not domain:
            break
        if domain == "#":
            break
            # continue
        if domain == "$":
            break
        domains.append(domain)
    return domains


def set_database(v_db, b_db, h_db):
    global visit_db, banner_db, html_db
    if v_db is None:
        visit_db = pd.DataFrame({
            'visit_id': pd.Series([], dtype='int'),
            'domain': pd.Series([], dtype='str'),
            'url': pd.Series([], dtype='str'),
            'run_url': pd.Series([], dtype='str'),
            'status': pd.Series([], dtype='int'),
            'btn_status': pd.Series([], dtype='int'),
            'lang': pd.Series([], dtype='str'),
            'banners': pd.Series([], dtype='int'),
            'ttw': pd.Series([], dtype='int'),
            '__cmp': pd.Series([], dtype='bool'),
            '__tcfapi': pd.Series([], dtype='bool'),
            '__tcfapiLocator': pd.Series([], dtype='bool'),
            'cmp_id': pd.Series([], dtype='int'),
            'cmp_name': pd.Series([], dtype='str'),
            'pv': pd.Series([], dtype='bool'),
            'dnsmpi': pd.Series([], dtype='str'),
            'body_html': pd.Series([], dtype='str'),
        })
        banner_db = pd.DataFrame({
            'banner_id': pd.Series([], dtype='int'),
            'visit_id': pd.Series([], dtype='int'),
            'domain': pd.Series([], dtype='str'),
            'lang': pd.Series([], dtype='str'),
            'iFrame': pd.Series([], dtype='bool'),
            'shadow_dom': pd.Series([], dtype='bool'),
            'captured_area': pd.Series([], dtype='float'),
            'x': pd.Series([], dtype='float'),
            'y': pd.Series([], dtype='float'),
            'w': pd.Series([], dtype='float'),
            'h': pd.Series([], dtype='float'),
        })
        html_db = pd.DataFrame({
            'banner_id': pd.Series([], dtype='int'),
            'visit_id': pd.Series([], dtype='int'),
            'domain': pd.Series([], dtype='str'),
            'html': pd.Series([], dtype='str'),
        })
    else:
        visit_db = v_db
        banner_db = b_db
        html_db = h_db
    return visit_db, banner_db, html_db


def get_database():
    global visit_db, banner_db, html_db
    return visit_db, banner_db, html_db


def open_domain_page(domain, sleep=TEST_MODE_SLEEP):
    global driver, this_url, this_domain, this_status
    mode = 1
    while True:
        url = make_url(domain, mode)
        if url == '':
            break
        try:
            driver.get(url)
            this_status = 0
            time.sleep(sleep)
            break
        except TimeoutException as ex:
            with open(log_file, 'a+') as f:
                print("failed to get (TimeOut): " +
                      url + " " + ex.__str__(), file=f)
                MyExceptionLogger(err=ex, file=f)
            this_status = 1
        except WebDriverException as ex:
            with open(log_file, 'a+') as f:
                print("failed to get (unreachable): " +
                      url + " " + ex.__str__(), file=f)
                MyExceptionLogger(err=ex, file=f)
            this_status = 2
        finally:
            mode += 1
    this_domain = domain
    this_url = url
    return url


def find_cookie_banners(origin_el=None, translate=False, stale_flag=False):
    # TODO: WITHOUT EXCEPTION HANDLING
    global driver, this_lang
    try:
        banners = []
        banners_map = dict()

        if origin_el is None:
            wait = WebDriverWait(driver, 5)
            body_el = wait.until(
                ec.visibility_of_element_located((By.TAG_NAME, "body")))
            time.sleep(2)
            WebDriverWait(driver, 30).until(lambda d: d.execute_script(
                'return document.readyState') == 'complete')
            # body_el = driver.find_element(By.TAG_NAME, "body")
            origin_el = body_el
            shadowdom_flag = False
        else:
            shadowdom_flag = True
        if translate:
            detected_lang = this_lang
            els_with_cookie = find_els_with_cookie(origin_el, detected_lang)
        else:
            detected_lang = "en"
            # find all the element with cookies related words
            els_with_cookie = find_els_with_cookie(origin_el)
        if els_with_cookie:
            banners_map = find_fixed_ancestors(els_with_cookie)
            if not banners_map:
                banners_map = find_by_zindex(els_with_cookie)
            if not banners_map:
                banners_map[origin_el] = find_deepest_el(els_with_cookie)
            for item in banners_map.items():
                optimal_el = find_optimal(driver, item)
                if is_inside_viewport(optimal_el) and has_enough_word(optimal_el) and not is_signin_banner(optimal_el):
                    banners.append(optimal_el)
        # check all the iframes to detect cookie banners
        frame_pairs = find_CMP_cookies_iframes(driver, detected_lang)
        for frame_pair in frame_pairs:
            # check if the banner is in viewport
            if is_inside_viewport(frame_pair[0]):
                banners.append(frame_pair)
        if not banners and not shadowdom_flag:
            shadowdom_banners = find_shadowdom_banners(driver)
            for dom_pair in shadowdom_banners:
                banners.append(dom_pair)
                # if is_inside_viewport(dom_pair[0]):  # check if the banner is in viewport

        return banners
    except StaleElementReferenceException:  # double chance if the page is refreshed or changed
        time.sleep(0.5)
        if not stale_flag:
            return find_cookie_banners(stale_flag=True)
        raise
    except:
        raise
        # return banners


def find_shadowdom_banners(driver):
    banners = []
    # root_copy_pairs_js = add_shadow_dom_to_body(driver)
    root_copy_pairs = add_shadow_dom_to_body(driver)
    for root_copy_pair in root_copy_pairs:
        shadow_dom_banner = find_cookie_banners(origin_el=root_copy_pair[1])
        if shadow_dom_banner:
            banners.append((root_copy_pair[0], shadow_dom_banner[0]))

    return banners


def detect_banners(data):  # return banners of the current running url
    global driver, this_url, this_domain, this_status, visit_db, this_lang, this_index
    banners = []
    inc_counter()
    try:
        if ZOOMING:
            zoom_out(3)
        if not data.url:
            return banners
        this_index = data.index
        this_url = data.url
        this_domain = data.domain
        this_lang = None
        start_time = datetime.now()
        banners = find_cookie_banners()
        finish_time = datetime.now()
        completion_time = finish_time - start_time
        # with open(banners_log_file, 'a+') as f:
        #     init_str = this_domain + " banner detection finished within: " + str(
        #         completion_time.microseconds)
        #     print(init_str, file=f)

        this_lang = page_lang(driver)
        if ATTEMPTS:
            for att in range(ATTEMPTS):
                if banners:
                    break
                time.sleep(ATTEMPT_STEP)
                if not banners:
                    banners = find_cookie_banners()
                else:
                    return banners
                data.ttw = (att + 1) * ATTEMPT_STEP
        if not banners and TRANSLATION:
            # if no banner is found and the language of site is not english then translate the page and check again
            if "en" not in this_lang and is_in_langlist(this_lang):
                translate_page(driver)
                banners = find_cookie_banners(translate=True)
                this_status = 3
                data.status = this_status
    except Exception as ex:
        with open(log_file, 'a+') as f:
            print("failed to continue detecting banner for domain: " +
                  this_domain + " " + ex.__str__(), file=f)
            # MyExceptionLogger(err=ex, file=f)
        this_status = -1
        data.status = this_status
    return banners


# first opens the domain then detects banners of that domain
def open_domain_plus_detect_banner(domain):
    open_domain_page(domain)
    return detect_banners()


def interact_with_cmp_banner(el: WebElement):
    global driver, MODIFIED_ADDON
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # never_consent_extension_win_path = r'C:\Users\arasaii\AppData\Roaming\Mozilla\Firefox\Profiles\jf3srcbq.cookiesprofile\extensions\{816c90e6-757f-4453-a84f-362ff989f3e2}.xpi'  # Must be the full path to an XPI file!
    # Must be the full path to an XPI file!
    never_consent_extension_win_path = r'C:\Drives\Education\MPI\Intern\Codes\Workstation\bannerdetection\neverconsent\neverconsent.xpi'
    never_consent_extension_path = current_dir + "/neverconsent/neverconsent.xpi"
    if MODIFIED_ADDON:
        run_addon_js(driver)
    else:
        try:
            id = driver.install_addon(
                never_consent_extension_path, temporary=True)
        except:
            id = driver.install_addon(
                never_consent_extension_win_path, temporary=True)
    time.sleep(1.5)
    if not MODIFIED_ADDON:
        driver.uninstall_addon(id)
    try:
        if el.is_displayed():
            return False
        else:
            return True
    except Exception as E:
        return True


def interact_with_banner(banner_item, choice, status, i, total_search=False):
    global driver, this_index, NON_EXPLICIT, rej_flag, NC_ADDON, SIMPLE_DETECTION, SCREENSHOT
    flag = False
    addon_detection = False
    explicit_coeff = 1

    WebDriverWait(driver, 30).until(lambda d: d.execute_script(
        'return document.readyState') == 'complete')
    body_el = driver.find_element(By.TAG_NAME, "body")

    try:
        banner, shadow_host = get_banner_obj(banner_item)
    except Exception as ex:
        with open(log_file, 'a+') as f:
            print("failed in switching frame for : " + this_url + " in interact with banner. " + ex.__str__(),
                  file=f)
            MyExceptionLogger(err=ex, file=f)
        driver.switch_to.default_content()
        return
    try:
        if total_search:       # search the whole body DOM for the words, this is because sometimes for example after clicking on setting the banner DOM disappears or the page redirect to another page.
            el = body_el
        else:
            el = banner
    except:
        el = body_el

    try:
        file_name = create_btn_filename(choice, i)
        ex_btns = extract_btns(el, choice, shadow_root=shadow_host)
        if choice == 2 and rej_flag and len(ex_btns) > 3:
            keep_els_with_words(
                ex_btns, ['all'], this_banner_lang, check_attr=False)
        ex_btns_temp = list(ex_btns)
        if SIMPLE_DETECTION or choice != 2 or rej_flag:
            flag = click_func(ex_btns, file_name, SCREENSHOT)
            if not flag and NON_EXPLICIT:
                nex_btns = extract_btns(
                    el, choice, shadow_root=shadow_host, non_explicit=True)
                entries_to_remove(ex_btns_temp, nex_btns)
                flag = click_func(nex_btns, file_name, SCREENSHOT)
                explicit_coeff = -1

        if choice == 2 and not flag and not rej_flag:
            if REJ_IN_SET:
                set_flag = interact_with_banner(el, 3, status, i)
                if set_flag:
                    rej_flag = True
                    # this total_search causes click on wrong reject btns like: statista.com, politico.com, sap.com
                    flag = interact_with_banner(
                        el, choice, status, i, total_search=True)
            set_flag = False
            if NC_ADDON and not flag:
                set_flag = interact_with_cmp_banner(el)
            if set_flag:
                addon_detection = True

        if type(banner_item) is tuple:
            driver.switch_to.default_content()
        if flag:
            if choice == 1 or choice == 2:
                status['btn_status'] = choice * explicit_coeff
                take_current_page_sc(suffix=suffix(
                    choice) + "_after" + str(i + 1))
            elif choice == 3:
                status['btn_set_status'] = choice * explicit_coeff
                take_current_page_sc(suffix=suffix(
                    choice) + "_after" + str(i + 1))
            elif choice == 4:
                status['btn_status'] = choice * explicit_coeff
                click_on_contentpass_continue(driver, i)
            if shadow_host:
                del_cloned_shadow_hosts(driver)
        if addon_detection:
            status['btn_set_status'] = 1
            take_current_page_sc(suffix="_Xnc_after" + str(i + 1))
    except Exception as ex:
        with open(log_file, 'a+') as f:
            print("failed in interact with banner for : " +
                  this_url + "  " + ex.__str__(), file=f)
            MyExceptionLogger(err=ex, file=f)
        driver.switch_to.default_content()

    return flag


def extract_btns(element, choice, shadow_root=None, non_explicit=False):
    global this_banner_lang
    if choice == 1:
        btns = find_btns_by_list(
            element, accept_words, this_banner_lang, non_explicit)
        remove_els_with_words(btns, non_acceptable, this_banner_lang)
    elif choice == 2:
        btns = find_btns_by_list(
            element, reject_words, this_banner_lang, non_explicit)
    elif choice == 3:
        btns = find_btns_by_list(
            element, setting_words, this_banner_lang, non_explicit)
    elif choice == 4:
        btns = find_btns_by_list(element, login_words,
                                 this_banner_lang, non_explicit)

    if shadow_root is not None:
        btns = get_els_from_root(shadow_root, btns)
    return btns


def suffix(choice):
    global rej_flag
    if choice == 1:
        return "_XX" + 'acc'
    elif choice == 2:
        return "_XX" + 'rej' + ('INset' if rej_flag else '')
    elif choice == 3:
        return "_X" + 'set'
    elif choice == 4:
        return "_X" + 'log'
    elif choice == 5:
        return "_XX" + 'conbtn'
    elif choice == 6:
        return "_X" + 'log'
    elif choice == 7:
        return "_XX" + 'login'


def create_btn_filename(choice, i):
    global this_index
    return sc_dir + get_sc_file_name(this_index) + suffix(choice) + "_" + str(i + 1)


def get_banner_obj(banner_item):
    global driver
    shadow_host = None
    if type(banner_item) is tuple:
        frame = banner_item[0]
        banner = banner_item[1]
        try:
            driver.switch_to.frame(frame)
            if type(banner) is tuple:
                frame = banner[0]
                banner = banner[1]
                driver.switch_to.frame(frame)
        except:  # for shadow root
            banner = banner_item[1]
            shadow_host = banner_item[0]
    else:
        banner = banner_item
    return banner, shadow_host


def get_sc_file_name(index=None, url=None):
    global driver, visit_db, this_url
    if url is None:
        url = this_url
    if index is None:
        return str(visit_db.shape[0]) + " " + get_current_domain(driver, url)
    else:
        return str(index+1) + " " + get_current_domain(driver, url)


def take_current_page_sc(data=None, directory=None, suffix=""):
    global driver, SCREENSHOT
    if SCREENSHOT:
        if data is None:
            index = this_index
            url = this_url
        else:
            index = data.index
            url = data.url
        if directory is None:
            directory = sc_dir
        try:
            driver.save_screenshot(
                directory + get_sc_file_name(index, url) + suffix + ".png")
        except Exception as ex:
            with open(log_file, 'a+') as f:
                print("failed to take screenshot for domain: " +
                      data.domain + " " + ex.__str__(), file=f)
                # MyExceptionLogger(err=ex, file=f)


def inc_counter():
    global counter
    counter += 1


def take_banner_sc(banner_item, data, j=None):
    if banner_item:
        try:
            banner, _ = get_banner_obj(banner_item)
        except Exception as ex:
            with open(log_file, 'a+') as f:
                print("failed in switching in banner_sc section for : " +
                      this_url + " " + ex.__str__(), file=f)
                MyExceptionLogger(err=ex, file=f)
            return
        try:
            if j is not None:
                if CHROME:
                    # chrome does not have built-in function for taking screenshot of an element.
                    chrome_element_sc(banner, data.index, j)
                else:
                    banner.screenshot(
                        sc_dir + get_sc_file_name(this_index) + "_banner" + str(j + 1) + ".png")
            else:
                banner.screenshot(
                    sc_dir + get_sc_file_name(this_index) + "_banner" + ".png")
            if type(banner_item) is tuple:
                driver.switch_to.default_content()
        except Exception as ex:
            with open(log_file, 'a+') as f:
                print("failed in switching in banner_sc section for : " +
                      this_url + " " + ex.__str__(), file=f)
                # MyExceptionLogger(err=ex, file=f)
        return banner


def chrome_element_sc(banner, index, j):
    location = banner.location
    size = banner.size
    ax = location['x']
    ay = location['y']
    width = location['x'] + size['width']
    height = location['y'] + size['height']
    crop_image = Image.open(sc_dir + get_sc_file_name(index) + ".png")
    crop_image = crop_image.crop((int(ax), int(ay), int(width), int(height)))
    crop_image.save(sc_dir + get_sc_file_name(index) +
                    "_banner" + str(j + 1) + "_ch.png")


def extract_banner_data(banner_item):
    global driver
    banner_data = {}

    try:
        banner, shadow_host = get_banner_obj(banner_item)
    except Exception as ex:
        with open(log_file, 'a+') as f:
            print("failed in switching frame for : " + this_url +
                  " in exctact banner data. " + ex.__str__(), file=f)
            # MyExceptionLogger(err=ex, file=f)
        driver.switch_to.default_content()
        return

    try:
        banner_data["captured_area"] = calc_area(
            list(banner.size.values())) / calc_area(list(get_win_inner_size(driver)))
        banner_data["x"] = banner.location["x"]
        banner_data["y"] = banner.location["y"]
        banner_data["w"] = banner.size["width"]
        banner_data["h"] = banner.size["height"]
        banner_data['html'] = to_html(banner)
        banner_data['lang'] = detect_lang(banner.text)
        if type(banner_item) is tuple:
            if shadow_host is not None:
                banner_data["shadow_dom"] = True
            else:
                banner_data["iFrame"] = True
                driver.switch_to.default_content()
        else:
            banner_data["iFrame"] = False
            banner_data["shadow_dom"] = False
    except Exception as ex:
        with open(log_file, 'a+') as f:
            print("failed in extracting banner for : " +
                  this_url + " " + ex.__str__(), file=f)
            # MyExceptionLogger(err=ex, file=f)
        return

    return banner_data


def get_data_dicts(banner_data):
    global this_domain, visit_db, banner_db, html_db, this_index
    try:
        visit_id = this_index
        banner_id = random.getrandbits(53)
        b_row_dict = {'banner_id': banner_id,
                      'visit_id': visit_id, 'domain': this_domain}
        h_row_dict = {'banner_id': banner_id,
                      'visit_id': visit_id, 'domain': this_domain}
        b_row_dict.update(banner_data)
        h_row_dict['html'] = banner_data["html"]
        del b_row_dict['html']

        banner_db.loc[banner_db.shape[0],
                      b_row_dict.keys()] = b_row_dict.values()
        html_db.loc[html_db.shape[0], h_row_dict.keys()] = h_row_dict.values()
    except Exception as ex:
        with open(log_file, 'a+') as f:
            print("failed to continue extracting banner data for domain: " +
                  this_url + " " + ex.__str__(), file=f)
            MyExceptionLogger(err=ex, file=f)
    finally:
        return b_row_dict, h_row_dict


def take_banners_sc(banners, data):
    if banners:
        for j, banner_item in enumerate(banners):
            try:
                take_banner_sc(banner_item, data, j)
            except Exception as ex:
                with open(log_file, 'a+') as f:
                    print("failed to continue in taking banner sc for domain: " + this_url + " " + ex.__str__(),
                          file=f)
                    MyExceptionLogger(err=ex, file=f)
    elif NOBANNER_SC:
        take_current_page_sc(data, nobanner_sc_dir)


def extract_banners_data(banners):
    banners_data = []
    for banner_item in banners:
        banner_data = extract_banner_data(banner_item)
        if banner_data:
            banners_data.append(banner_data)
    return banners_data


def set_data_in_db_error(data):
    global this_domain
    try:
        set_data_in_db(data)
    except Exception as ex:
        with open(log_file, 'a+') as f:
            print("failed to continue setting data in DB for domain: " +
                  this_domain + " " + ex.__str__(), file=f)
            MyExceptionLogger(err=ex, file=f)


def set_data_in_db(data):
    global driver, this_url, this_domain, this_status, this_lang, visit_db, this_run_url
    if data.openwpm:
        visit_id = data.index
        this_status = data.status
        this_url = data.url
        this_domain = get_current_domain(driver, this_url)
    else:
        visit_id = visit_db.shape[0] + 1
    try:
        run_url = driver.current_url
    except Exception as ex:
        run_url = None
        with open(log_file, 'a+') as f:
            print("run_url/diver.current_url is not available when saving the Data " +
                  this_domain + " " + ex.__str__(), file=f)
        MyExceptionLogger(err=ex, file=f)
    v_dict = {'visit_id': visit_id, 'domain': this_domain, 'url': this_url, 'run_url': run_url, 'status': this_status, 'lang': this_lang, 'banners': num_banners,
              'btn_status': data.btn_status['btn_status'], 'btn_set_status': data.btn_status['btn_set_status'], 'interact_time': data.interact_time, 'ttw': data.ttw, '__tcfapi': False, '__tcfapiLocator': False, 'pv': False, 'nc_cmp_name': data.nc_cmp_name}

    try:
        body_html = to_html(driver.find_element(By.TAG_NAME, "body"))
    except:
        body_html = None
    v_dict['dnsmpi'] = dnsmpi_detection(body_html)
    if SAVE_BODY:
        v_dict['body_html'] = body_html
    else:
        v_dict['body_html'] = None
    b_dict = {}
    h_dict = {}
    # not equal with: visit_db = visit_db.append(row_dict, ignore_index=True), using second one, new dataframe with new address will be created.
    visit_db.loc[visit_db.shape[0], v_dict.keys()] = v_dict.values()

    for banner_data in data.banners_data:
        b_dict, h_dict = get_data_dicts(banner_data)
        if data.openwpm:
            data.save_record_in_sql("banners", b_dict)
            if SAVE_HTML:
                data.save_record_in_sql("htmls", h_dict)

    CMP_dict = cd.extract_CMP_data(data.CMP)
    v_dict.update(CMP_dict)
    if data.openwpm:
        data.save_record_in_sql("visits", v_dict)

    return v_dict, b_dict, h_dict


def halt_for_sleep(data):
    if data.start_time:
        while True:
            cur_time = datetime.now()
            completion_time = cur_time - data.start_time
            insec = completion_time.total_seconds()
            if insec < data.sleep:
                time.sleep(0.5)
            else:
                data.finish_time = cur_time
                break


def enter_user_pass(driver):
    wait = WebDriverWait(driver, 3)
    email_in = wait.until(ec.visibility_of_element_located(
        (By.XPATH, '//*[@id="email"]')))
    pass_in = driver.find_element(By.XPATH, '//*[@id="password"]')
    email_in.send_keys("aarasaa01@gmail.com")
    pass_in.send_keys("@ASD123123asd")
    login_btn = driver.find_element(
        By.XPATH, '/html/body/div[1]/div/div[2]/div/div/div[1]/div[2]/div/div[1]/form/button')
    login_btn.click()


def click_on_contentpass_continue(driver, i):
    try:
        click_continue_btn(driver, i)
        return True
    except:
        try:
            take_current_page_sc(suffix=suffix(6) + "_after" + str(i + 1))
            enter_user_pass(driver)
            click_continue_btn(driver, i)
            return True
        except Exception as ex:
            with open(log_file, 'a+') as f:
                print("failed in clicking on contentpass continue for : " + this_url + " in interact with banner. " + ex.__str__(),
                      file=f)
                MyExceptionLogger(err=ex, file=f)
            return False


def click_continue_btn(driver, i):
    wait = WebDriverWait(driver, 3)
    continue_btn = wait.until(ec.visibility_of_element_located(
        (By.XPATH, '//*[@id="container"]/div/div[2]/div/div/div[1]/div[2]/div/div/form/div/button')))
    take_current_page_sc(suffix=suffix(5) + "__before" + str(i + 1))
    continue_btn.click()
    time.sleep(0.5)
    body_el = wait.until(ec.visibility_of_element_located(
        (By.TAG_NAME, 'body')))
    time.sleep(0.5)
    take_current_page_sc(suffix=suffix(5) + "_after" + str(i + 1))


def interact_with_banners(data, choice):  # choices: 1.accept 2.reject
    global rej_flag, this_banner_lang, this_interact_time
    for i, banner in enumerate(data.banners):
        # btn_status: 1. accept 2. reject; btn_set_status: 3. setting 1. add-on; for all if neg then it is non-explicit;
        btn_status = {"btn_status": None, "btn_set_status": None}
        this_banner_lang = data.banners_data[i]['lang']
        if choice:
            interact_with_banner(banner, choice, btn_status, i)
            data.nc_cmp_name = get_cmp_name_nc(driver)

        data.btn_status = btn_status
        rej_flag = False
        data.interact_time = time.time() * 1000


def run_banner_detection(data, sc=SCREENSHOT):
    global num_banners, driver, this_start_time
    data.domain = get_current_domain(driver, data.url)
    banners = detect_banners(data)
    num_banners = len(banners)
    if sc:
        take_current_page_sc(data)
        take_banners_sc(banners, data)
    return banners


def save_database():
    global visit_db, banner_db, html_db
    if visit_db is not None:
        visit_db.to_csv(data_dir + '/visits.csv', index=False)
        banner_db.to_csv(data_dir + '/banners.csv', index=False)
        html_db.to_csv(data_dir + '/htmls.csv', index=False)

        init_str = "(saving) visits_db id is: {},\n db is: {}".format(
            id(visit_db), visit_db)
        with open(data_dir + "/sites.txt", 'a+') as f:
            print(init_str, file=f)


def set_mode(file_name, var, run_mode=0):
    global season_dir, custom_dir, time_dir, time_or_custom, data_dir, sc_dir, nobanner_sc_dir, sc_file_name, log_file, banners_log_file
    if run_mode:
        DETECT_MODE = run_mode  # fixed = 1, z-index = 2, custom set = 0
        if run_mode == 1:
            custom_dir = "fixed"
        elif run_mode == 2:
            custom_dir = "zindex"
        time_or_custom = custom_dir
    else:
        time_or_custom = datetime.now().date().__str__() + \
            datetime.now().strftime(" %H-%M-%S").__str__() + "--" + file_name + "-" + var

    data_dir = season_dir + time_or_custom
    sc_dir = data_dir + "/screenshots/"
    nobanner_sc_dir = sc_dir + "nobanner/"
    sc_file_name = ""
    log_file = data_dir + '/logs.txt'
    banners_log_file = data_dir + '/banners_log.txt'


def run_all(dmns=None):   # this function is used for run the banner detection module only (Not through OpenWPM)
    global driver, counter
    if dmns is None:
        dmns = get_domains()

    for domain in dmns:
        # banners = open_domain_plus_detect_banner(domain)
        url = open_domain_page(domain)
        run_all_for_domain(domain, url)
    else:
        time.sleep(2)
        close_driver()


def run_all_for_domain(DMN, URL):
    global counter, SLEEP_TIME
    try:
        class Data:
            url = URL
            domain = DMN
            banners = []
            banners_data = []
            CMP = {}
            index = None
            sleep = SLEEP_TIME
            ttw = 0   # time to wait (to show the banner)
            status = None
            btn_status = None
            openwpm = False
            btn_status = {"btn_status": None, "btn_set_status": None}
            nc_cmp_name = None
            interact_time = None
            start_time = datetime.now()
            finish_time = 0

        Data.index = visit_db.shape[0]
        if BANNERCLICK:
            banners = run_banner_detection(Data)
            Data.banners = banners
            Data.banners_data = extract_banners_data(banners)
        if CMPDETECTION:
            Data.CMP = cd.run_cmp_detection()
        if BANNERINTERACTION:
            interact_with_banners(Data, CHOICE)
        if SLEEP_AFTER_INTERACTION:
            Data.start_time = datetime.now()
        set_data_in_db(Data)
        halt_for_sleep(Data)

    except MemoryError as ex:
        visit_db.loc[visit_db.index[-1], 'status'] = -1
        with open(log_file, 'a+') as f:
            print('Memory Error happened for: ' + DMN + "  " + ex.__str__(),
                  file=f)
            MyExceptionLogger(err=ex, file=f)
    except InvalidSessionIdException as ex:
        visit_db.loc[visit_db.index[-1], 'status'] = -1
        with open(log_file, 'a+') as f:
            print('InvalidSessionIdException happened for: ' + DMN + "  " + ex.__str__(),
                  file=f)
            MyExceptionLogger(err=ex, file=f)
        raise
    except Exception as ex:
        visit_db.loc[visit_db.index[-1], 'status'] = -1
        with open(log_file, 'a+') as f:
            print('Exception happened for: ' + DMN + "  " + ex.__str__(),
                  file=f)
            MyExceptionLogger(err=ex, file=f)
    # finally:
    #     if not (counter % 100):
    #         print(str(counter) + ' websites have been crawled successfully! The last one was: ', domain)


def close_driver():
    global driver
    save_database()
    driver.quit()
    reset()


if __name__ == '__main__':
    # this function is used for run the banner detection module only (Not through OpenWPM)

    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", nargs='+',
                        help="list of file paths contains URLs that will be run sequentially (attached to the name of data folder)")
    parser.add_argument(
        "-v", "--variable", help="variable of run (attached to the name of data folder)")
    parser.add_argument('--headless', action='store_true',
                        help="start on headless mode")
    args = parser.parse_args()
    files = args.file
    variable = args.variable
    HEADLESS = args.headless

    if not files:
        files = [urls_file, "AlexaTop1kGlobal.txt", "addon_urls.txt"]
        files = [urls_file]
        variable = 'test'
    try:
        for f in files:
            set_mode(f, variable, 0)
            init(f)
            cd.init(driver, get_database()[0])
            run_all()
    except:
        if driver:
            close_driver()
        raise
