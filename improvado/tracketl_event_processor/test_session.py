# -*- coding: utf-8 -
from datetime import datetime, timedelta
from decimal import Decimal
from copy import deepcopy
from unittest import TestCase
from event_processor.session import SessionEventProcessor


class SessionEventProcessorTest(TestCase):
    the_event = None
    the_session = None
    empty_utms = None
    # show full diff
    maxDiff = None

    def setUp(self):
        self.the_event = {
            'cleaned_url': u'https://www.realcrowd.com/portfolio',
            'cookie_id': u'd7914270362111e5a4dc07c50b666053',
            'js': True,
            'order_info': None,
            'pixel_id': u'5B66AE6',
            'group_id': u'5B66AE6',
            'time': datetime(2015, 11, 24, 10, 15, 1, 253000),
            'ua': u'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.86 Safari/537.36',
            'url': u'https://www.realcrowd.com/portfolio?utm_source=submitted-investor-followup&utm_medium=email&utm_content=utm%2Fcontent&utm_term=%20utm_term%20&utm_campaign=realcrowd-subscriber-most_recent_commitment_offering_title-default-commitment-next-steps&__s=cgobbqjbj7nzj3gzs2zs',
            'utms': {
                'utm_campaign': u'realcrowd-subscriber-most_recent_commitment_offering_title-default-commitment-next-steps',
                'utm_content': u'utm/content',
                'utm_medium': u'email',
                'utm_source': u'submitted-investor-followup',
                'utm_term': u'utm_term'
            },
            'for_page': True
        }
        self.the_session = {
            'conversions': {},
            'pages': {(u'5B66AE6', u'https://www.realcrowd.com/portfolio'): 1},
            'last': datetime(2015, 11, 24, 10, 15, 1, 253000),
            'page_count': 1,
            'landing': u'https://www.realcrowd.com/portfolio',
            'start': datetime(2015, 11, 24, 10, 15, 1, 253000),
            'utms': {
                'utm_campaign': u'realcrowd-subscriber-most_recent_commitment_offering_title-default-commitment-next-steps',
                'utm_term': u'utm_term', 'utm_medium': u'email', 'utm_source': u'submitted-investor-followup',
                'utm_content': u'utm/content'
            }
        }
        self.empty_utms = {
            'utm_campaign': u'',
            'utm_content': u'',
            'utm_medium': u'',
            'utm_source': u'',
            'utm_term': u''
        }

    #
    # user_event_to_session method
    #
    def test_user_events_to_profile_should_sort_events(self):
        # events with reversed sort by time
        event1 = self.the_event
        event2 = deepcopy(self.the_event)
        event2['time'] = event1['time'] - SessionEventProcessor.SESSION_PERIOD*2
        user_events = [event1, event2]
        # expected sessions
        session1 = deepcopy(self.the_session)
        session1['start'] = session1['last'] = event1['time'] - SessionEventProcessor.SESSION_PERIOD*2
        session2 = deepcopy(self.the_session)
        exp = {u'5B66AE6': [session1, session2]}
        self.assertEqual(exp, SessionEventProcessor.user_events_to_profile(user_events))

    def test_user_events_to_profile_should_skip_events_without_group_id(self):
        event1 = deepcopy(self.the_event)
        event1['group_id'] = None
        user_events = [event1]
        exp = {}
        self.assertEqual(exp, SessionEventProcessor.user_events_to_profile(user_events))


    def test_user_events_to_profile_should_group_sessions_by_group_id(self):
        # events
        event1 = self.the_event
        event2 = deepcopy(self.the_event)
        event2['pixel_id'] = event2['group_id'] = u'PIXEL_B'
        user_events = [event1, event2]
        # expected sessions
        session2 = deepcopy(self.the_session)
        session2['pages'] = {(u'PIXEL_B', u'https://www.realcrowd.com/portfolio'): 1}
        exp = {
            u'5B66AE6': [self.the_session],
            u'PIXEL_B': [session2]
        }
        self.assertEqual(exp, SessionEventProcessor.user_events_to_profile(user_events))

    def test_user_events_to_profile_should_process_global_and_conversion_pixels(self):
        # events
        event1 = self.the_event
        event2 = deepcopy(self.the_event)
        event2['pixel_id'] = u'PIXEL_CONV'
        user_events = [event1, event2]
        # expected sessions
        exp_session = deepcopy(self.the_session)
        exp_session['conversions'] = {
            u'PIXEL_CONV': {
                'page_count': 1,
                'orders_info': []
            }
        }
        exp_session['page_count'] = 2
        exp_session['pages'][(u'PIXEL_CONV', u'https://www.realcrowd.com/portfolio')] = 1
        exp = {
            u'5B66AE6': [exp_session]
        }
        self.assertEqual(exp, SessionEventProcessor.user_events_to_profile(user_events))

    #
    # _process_group_sessions_event method
    #
    def test__process_group_sessions_event_create_first_session(self):
        exp_group_sessions = [self.the_session]
        group_sessions = []
        self.assertEqual(
            exp_group_sessions,
            SessionEventProcessor._process_group_sessions_event(group_sessions, self.the_event))

    def test__process_group_sessions_event_update_session(self):
        group_sessions = [deepcopy(self.the_session)]
        event = deepcopy(self.the_event)
        event['time'] += timedelta(seconds=30)
        event['url'] = event['cleaned_url'] = u'https://www.realcrowd.com/'
        event['utms'] = self.empty_utms
        exp_session = deepcopy(self.the_session)
        exp_session['last'] = event['time']
        exp_session['page_count'] = 2
        exp_session['pages'][(u'5B66AE6', u'https://www.realcrowd.com/')] = 1
        exp_group_sessions = [exp_session]
        self.assertEqual(
            exp_group_sessions,
            SessionEventProcessor._process_group_sessions_event(group_sessions, event))

    def test__process_group_sessions_event_create_new_session(self):
        group_sessions = [deepcopy(self.the_session)]
        event = deepcopy(self.the_event)
        event['time'] += SessionEventProcessor.SESSION_PERIOD + timedelta(seconds=1)
        event['url'] = event['cleaned_url'] = u'https://www.realcrowd.com/'
        event['utms'] = self.empty_utms
        exp_session1 = deepcopy(self.the_session)
        exp_session2 = deepcopy(self.the_session)
        exp_session2['last'] = exp_session2['start'] = event['time']
        exp_session2['landing'] = event['cleaned_url']
        exp_session2['utms'] = self.empty_utms
        exp_session2['pages'] = {(u'5B66AE6', u'https://www.realcrowd.com/'): 1}
        exp_group_sessions = [exp_session1, exp_session2]
        self.assertEqual(
            exp_group_sessions,
            SessionEventProcessor._process_group_sessions_event(group_sessions, event))

    #
    # _update_session_conversions method
    #
    def test__update_session_conversions_no_conversion_for_global_pixel(self):
        session = deepcopy(self.the_session)
        exp_session = deepcopy(self.the_session)
        self.assertEqual(
            exp_session,
            SessionEventProcessor._update_session_conversions(session, self.the_event))

    def test__update_session_conversions_set_conversions_for_conversion_pixel(self):
        event = deepcopy(self.the_event)
        event['pixel_id'] = u'PIXEL_CONV'
        session = deepcopy(self.the_session)
        exp_session = deepcopy(self.the_session)
        exp_session['conversions'] = {
            u'PIXEL_CONV': {
                'page_count': 1,
                'orders_info': []
            }
        }
        self.assertEqual(
            exp_session,
            SessionEventProcessor._update_session_conversions(session, event))

    def test__update_session_conversions_add_orders_info(self):
        event1 = deepcopy(self.the_event)
        event1['pixel_id'] = u'PIXEL_CONV'
        event1['order_info'] = {'osum': Decimal('1.2'), 'oid': 'A'}
        event2 = deepcopy(self.the_event)
        event2['pixel_id'] = u'PIXEL_CONV'
        event2['order_info'] = {'osum': Decimal('2.1'), 'oid': 'A'}
        session = deepcopy(self.the_session)
        exp_session = deepcopy(self.the_session)
        exp_session['conversions'] = {
            u'PIXEL_CONV': {
                'page_count': 2,
                'orders_info': [
                    {'osum': Decimal('1.2'), 'oid': 'A'},
                    {'osum': Decimal('2.1'), 'oid': 'A'}
                ]
            }
        }
        session = SessionEventProcessor._update_session_conversions(session, event1)
        session = SessionEventProcessor._update_session_conversions(session, event2)
        self.assertEqual(exp_session, session)

    #
    # _should_start_new_session method
    #
    def test__should_start_new_session_true_when_no_last_session(self):
        # if no last_session, should create new session
        self._assert_should_start_new_session(True, None, self.the_event)

    def test__should_start_new_session_true_when_last_page_view_older_than_SESSION_PERIOD(self):
        # if last_session last page_view older than SESSION_PERIOD, should create new session
        last_session = deepcopy(self.the_session)
        last_session['last'] = last_session['last'] - (SessionEventProcessor.SESSION_PERIOD + timedelta(seconds=1))
        self._assert_should_start_new_session(True, last_session, self.the_event)

    def test__should_start_new_session_true_when_event_utms_are_different(self):
        # if event utms are not empty and different with last_session utms, should create new session
        last_session = deepcopy(self.the_session)
        event = deepcopy(self.the_event)
        event['utms']['utm_source'] = u"CHANGED_UTM_SOURCE"
        self._assert_should_start_new_session(True, last_session, event)

    def test__should_start_new_session_false_when_event_utms_are_empty(self):
        # if event utms are empty, should not create new session
        last_session = deepcopy(self.the_session)
        event = deepcopy(self.the_event)
        event['utms'] = self.empty_utms
        self._assert_should_start_new_session(False, last_session, event)

    def test__should_start_new_session_false(self):
        # else should continue session
        last_session = deepcopy(self.the_session)
        event = deepcopy(self.the_event)
        self._assert_should_start_new_session(False, last_session, event)

    def _assert_should_start_new_session(self, exp, last_session, event):
        self.assertEqual(exp, SessionEventProcessor._should_start_new_session(last_session, event))

    #
    # _update_session_pages method (session, event)
    #
    def test__update_session_pages_create_one_more_page_with_different_url(self):
        session = deepcopy(self.the_session)
        event = deepcopy(self.the_event)
        event['cleaned_url'] = u'https://www.realcrowd.com/'
        exp = deepcopy(session)
        exp['pages'][(u'5B66AE6', u'https://www.realcrowd.com/')] = 1
        self.assertEqual(exp, SessionEventProcessor._update_session_pages(session, event))

    def test__update_session_pages_create_update_page_with_same_url(self):
        session = deepcopy(self.the_session)
        event = deepcopy(self.the_event)
        exp = deepcopy(session)
        exp['pages'] = {(u'5B66AE6', u'https://www.realcrowd.com/portfolio'): 2}
        self.assertEqual(exp, SessionEventProcessor._update_session_pages(session, event))

    def test__update_session_pages_create_skip_page_without_mark(self):
        # for_page is not set
        session = deepcopy(self.the_session)
        event = deepcopy(self.the_event)
        del event['for_page']
        exp = deepcopy(session)
        self.assertEqual(exp, SessionEventProcessor._update_session_pages(session, event))

    def test__update_session_pages_create_skip_page_without_mark2(self):
        # for_page = False
        session = deepcopy(self.the_session)
        event = deepcopy(self.the_event)
        event['for_page'] = False
        exp = deepcopy(session)
        self.assertEqual(exp, SessionEventProcessor._update_session_pages(session, event))
