# -*- coding: utf-8 -*-
# Group events into sessions.
from datetime import timedelta
from event_processor import EventProcessor


class SessionEventProcessor(EventProcessor):
    PROFILE_PART_NAME = 'session'
    # Maximum time between events in one session
    SESSION_PERIOD = timedelta(minutes=30)

    @classmethod
    def user_events_to_profile(cls, user_events):
        """
        Groups events for one user and date by group_id and session.
        Session start rules are described in the method _should_start_new_session.
        :param user_events: {list of dicts} user history, list or dicts returned by TrackLogParser.parse_line
        :returns: {dict of lists of dicts} sessions, grouped by group_id and ordered by session start time:
            {
                group_id: [
                    {
                        start, last, landing, page_count,
                        utms: {utm_source, utm_medium, utm_campaign, utm_term, utm_content},
                        conversions: {
                            conv_pixel_id: {
                                page_count,
                                orders_info: [ {oid, osum} ]
                            }
                        },
                        pages: {(pixel_id, cleaned_url): page_count}
                        # next field may be added to the first session by attach_pixel_stat:
                        # is_first: True
                        # next field may be added to the first session by attach_pixel_stat
                        # (this list is not required to be sorted by session start time):
                        # previous_sessions: {[
                        #    utms: {utm_source, utm_medium, utm_campaign, utm_term, utm_content}, landing, start
                        # ]}
                    }
                ]
            }
        """
        user_events = sorted(user_events, key=lambda e: e['time'])
        sessions_by_group = {}
        for event in user_events:
            group_id = event['group_id']
            if not group_id:
                continue
            sessions_by_group[group_id] = cls._process_group_sessions_event(sessions_by_group.get(group_id, []), event)
        return sessions_by_group

    @classmethod
    def _process_group_sessions_event(cls, group_sessions, event):
        """
        Add event to existing session or create new session.
        :param group_sessions: {list}
        :param event: {dict}
        :return: {list}
        """
        last_sess = group_sessions[-1] if group_sessions else None
        started_new_session = cls._should_start_new_session(last_sess, event)
        if started_new_session:
            cur_sess = cls._create_session(event)
        else:
            cur_sess = last_sess
        cur_sess = cls._update_session(cur_sess, event)
        cur_sess = cls._update_session_conversions(cur_sess, event)
        cur_sess = cls._update_session_pages(cur_sess, event)
        if started_new_session:
            group_sessions.append(cur_sess)
        else:
            group_sessions[-1] = cur_sess
        return group_sessions

    @staticmethod
    def _update_session(session, event):
        """
        Update session based on event data.
        :param session: {dict}
        :param event: {dict}
        :returns {dict}: updated session
        """
        if session['last'] < event['time']:
            session['last'] = event['time']
        session['page_count'] += 1
        return session

    @staticmethod
    def _update_session_conversions(session, event):
        """
        Update conversions in the session. Only for conversion pixels (where pixel_id != group_id).
        :param session: {dict}
        :param event: {dict}
        :returns {dict}: updated session
        """
        pixel_id = event['pixel_id']
        if pixel_id != event['group_id']:
            conv = session['conversions'].get(pixel_id, {"page_count": 0, "orders_info": []})
            conv["page_count"] += 1
            if event['order_info']:
                conv["orders_info"].append(event['order_info'])
            session['conversions'][pixel_id] = conv
        return session

    @staticmethod
    def _update_session_pages(session, event):
        # count stat for events with 'for_page' mark only
        if event.get('for_page'):
            page_key = (event['pixel_id'], event['cleaned_url'])
            session['pages'][page_key] = session['pages'].get(page_key, 0) + 1
        return session

    @classmethod
    def _should_start_new_session(cls, last_session, event):
        """
        Check if new session should be started or last session should be continued.
        Start new session if:
        - no session exists
        - date changed (not checking here because we process only one date)
        - time from last session is more than SESSION_PERIOD
        - event's utm_ values aren't empty and are different from last session values
        :param last_session: {dict}
        :param event: {dict}
        :return: {bool}
        """
        if not last_session:
            return True
        # assume that event occurs later than any existing sessions
        if event['time'] > last_session['last'] + cls.SESSION_PERIOD:
            return True
        if any(event['utms'].values()) and event['utms'] != last_session['utms']:
            return True
        return False

    @staticmethod
    def _create_session(event):
        return {
            'start': event['time'],
            'last': event['time'],
            'landing': event['cleaned_url'],
            'utms': event['utms'],
            'page_count': 0,
            'conversions': {},
            'pages': {}
        }
