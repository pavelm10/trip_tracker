import datetime
import numpy as np
import gpxpy

from utils import str2path, simple_logger, interpolate_timestamps
from geo import calculate_distance
from elastic_interface import ElasticAPI


FDIR = str2path(__file__).parent.resolve()
SEC2H = 1 / 3600
M2KM = 0.001
MPS2KPH = 3.6


class GpxTripTracker(ElasticAPI):

    GPS_DTYPE = [('lat', 'f8'), ('lon', 'f8'), ('ele', 'f8'), ('timestamp', 'O'), ('sid', 'i4'), ('pt_type', 'U16')]
    ODO_DTYPE = [('cum_dist_km', 'f8'), ('dist_m', 'f8'), ('avg_vel_kmh', 'f8'), ('elev_delta_m', 'f8'),
                 ('time_delta_s', 'f8'), ('total_time_h', 'f8'), ('elev_up_cum_m', 'f8'), ('elev_down_cum_m', 'f8')
                 ]
    MODES = ['bike', 'run', 'walk']
    STOP_DIST_FAST_MAX_M = 40
    STOP_DIST_SLOW_MAX_M = 24
    MIN_SEPARATION_CORRECTION_DIST_M = 30

    def __init__(self, transport_mode=None, track_file_path=None, ref_file_path=None, start=None, end=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.transport_mode = self._validate_transport_mode(transport_mode)
        self.track_file_path = str2path(track_file_path)
        self.ref_file_path = str2path(ref_file_path)
        self.start = datetime.datetime.fromisoformat(start) if start else start
        self.end = datetime.datetime.fromisoformat(end) if end else end
        self.log = simple_logger()

    def read_file(self, file_path):
        """reads GPX file and builds track points data structure"""
        if not file_path.exists():
            err_msg = f'{file_path} does not exist!'
            self.log.error(err_msg)
            raise FileExistsError(err_msg)

        track_points = list()
        pt_type = 'original'
        with open(file_path, 'r') as gpx_file:
            gpx = gpxpy.parse(gpx_file)
            for track in gpx.tracks:
                for sid, segment in enumerate(track.segments):
                    for point in segment.points:
                        if point.time is None:
                            timestamp = 0
                        else:
                            timestamp = datetime.datetime.utcfromtimestamp(point.time.timestamp())
                        track_points.append((point.latitude, point.longitude, point.elevation, timestamp, sid, pt_type))

        return np.array(track_points, dtype=self.GPS_DTYPE)

    def ingest(self, track_points, odometry):
        """main ingestion to the elastic index"""
        if isinstance(track_points[0]['timestamp'], datetime.datetime):
            trip_type = 'driven'
            trip_start_utc = track_points[0]['timestamp']
            trip_end_utc = track_points[-1]['timestamp']

        elif self.start is not None:
            trip_type = 'untracked'
            trip_start_utc = self.start
            trip_end_utc = self.end
        else:
            trip_type = 'planned'
            trip_start_utc = ''
            trip_end_utc = ''

        trip_length_km = odometry['cum_dist_km'][-1]
        trip_duration_h = odometry['total_time_h'][-1]
        trip_avg_vel_kph = trip_length_km / trip_duration_h if trip_duration_h > 0 else 0
        trip_max_elev_m = np.max(track_points['ele'])
        trip_min_elev_m = np.min(track_points['ele'])

        global_message = {"trip_id": self.track_file_path.stem,
                          "trip_type": trip_type,
                          "transport_mode": self.transport_mode,
                          "trip_source_gpx": self.track_file_path.as_posix(),
                          "trip_start_utc": trip_start_utc,
                          "trip_end_utc": trip_end_utc,
                          "trip_duration_h": trip_duration_h,
                          "trip_length_km": trip_length_km,
                          "trip_avg_vel_kph": trip_avg_vel_kph,
                          "trip_max_elev_m": trip_max_elev_m,
                          "trip_min_elev_m": trip_min_elev_m}

        if self.es_index is not None:
            self.index_exists()
            trip_exists = self.trip_exists(global_message['trip_id'])
            if trip_exists:
                self.log.warning(f"The trip {global_message['trip_id']} already exists in the index, skipping ingest")
                return global_message

            elif trip_exists is None:
                self.log.error("Trip ID query failed, cannot ingest")
                return global_message

            else:
                self.push(global_message)
                self.log.info('Ingesting trip points...')
                for idx, (pt, odo_sample) in enumerate(zip(track_points, odometry)):
                    data = self._ingest_geo_point(pt, odo_sample, trip_type, idx+1)
                    self.push(data)
        else:
            self.log.warning('The index is None, hence no ingest to ES')

        return global_message

    def _ingest_geo_point(self, track_pt, odo_sample, trip_type, point_id):
        """push track point data to the elastic"""
        point_timestamp_utc = track_pt['timestamp']
        if not isinstance(track_pt['timestamp'], datetime.datetime):
            point_timestamp_utc = self.start if self.start else datetime.datetime.utcnow()
        data = {"location": {"lat": track_pt['lat'], "lon": track_pt['lon']},
                "elevation_m": track_pt['ele'],
                "point_timestamp_utc": point_timestamp_utc,
                "trip_id": self.track_file_path.stem,
                "point_id": point_id,
                "trip_type": trip_type,
                "trip_source_gpx": self.track_file_path.as_posix(),
                "cumulative_distance_km": odo_sample['cum_dist_km'],
                "cumulative_time_h": odo_sample['total_time_h'],
                "average_velocity_kmh": odo_sample['avg_vel_kmh'],
                "point_type": track_pt['pt_type'],
                "transport_mode": self.transport_mode}
        return data

    def extract_odometry(self, track_points):
        """Odometry extraction"""
        odo = np.zeros((len(track_points)), dtype=self.ODO_DTYPE)

        if isinstance(track_points[0]['timestamp'], datetime.datetime):
            elev_up_cum = 0
            elev_down_cum = 0
            med_time_delta = np.median(np.diff(track_points['timestamp'])).total_seconds()

            for idx, pt in enumerate(track_points):
                try:
                    next_pt = track_points[idx+1]
                    dist = calculate_distance(pt, next_pt)
                    dt = (next_pt['timestamp'] - pt['timestamp']).total_seconds()
                    if self._check_stop(dt, dist, med_time_delta):
                        dt = med_time_delta
                    elev_delta = next_pt['ele'] - pt['ele'] if (next_pt['ele'] != 0 or pt['ele'] != 0) else 0
                    if elev_delta > 0:
                        elev_up_cum += elev_delta
                    else:
                        elev_down_cum -= elev_delta

                    odo[idx+1]['dist_m'] = dist
                    odo[idx+1]['elev_delta_m'] = elev_delta
                    odo[idx+1]['elev_up_cum_m'] = elev_up_cum
                    odo[idx+1]['elev_down_cum_m'] = elev_down_cum
                    odo[idx+1]['time_delta_s'] = dt

                except IndexError:
                    break

            odo['avg_vel_kmh'][1:] = (odo['dist_m'][1:] / odo['time_delta_s'][1:]) * MPS2KPH
            odo['total_time_h'] = (np.cumsum(odo['time_delta_s'])) * SEC2H
            odo['cum_dist_km'] = (np.cumsum(odo['dist_m'])) * M2KM
        else:
            total_time_s = 0
            if self.start is not None:
                total_time_s = (self.end - self.start).total_seconds()

            for idx, pt in enumerate(track_points):
                try:
                    next_pt = track_points[idx + 1]
                    dist = calculate_distance(pt, next_pt)
                    odo[idx + 1]['dist_m'] = dist

                except IndexError:
                    break

            odo['cum_dist_km'] = (np.cumsum(odo['dist_m'])) * M2KM
            odo['total_time_h'][-1] = total_time_s * SEC2H
            odo['avg_vel_kmh'][1:] = (odo['cum_dist_km'][-1] / odo['total_time_h'][-1]) if total_time_s else 0

        return odo

    def write_corrected_data(self, track_points):
        """writes corrected track_points to the GPX file"""
        gpx = gpxpy.gpx.GPX()

        gpx_track = gpxpy.gpx.GPXTrack()
        gpx.tracks.append(gpx_track)

        gpx_segment = gpxpy.gpx.GPXTrackSegment()
        gpx_track.segments.append(gpx_segment)

        for pt in track_points:
            gpx_segment.points.append(gpxpy.gpx.GPXTrackPoint(pt['lat'], pt['lon'], pt['ele'], pt['timestamp']))

        xml2write = gpx.to_xml()
        output_name = self.track_file_path.stem + '_corrected' + self.track_file_path.suffix
        output_path = self.track_file_path.parent / output_name

        with open(output_path.as_posix(), 'w') as f:
            f.write(xml2write)

    def extract_data(self):
        """extracts track_points and ref_points if ref_file_path defined"""
        ref_points = None
        track_points = self.read_file(self.track_file_path)
        if self.ref_file_path:
            ref_points = self.read_file(self.ref_file_path)
        return track_points, ref_points

    def run(self):
        """main method for data extraction, possible data correction, extraction of odometry and ingest to elastic"""
        track_pts, ref_pts = self.extract_data()
        if ref_pts is not None:
            track_pts = self.correct_track_points(track_pts, ref_pts)
            self.write_corrected_data(track_pts)
        odo = self.extract_odometry(track_pts)
        overview = self.ingest(track_pts, odo)
        self.log.info(f'{overview}')

    def correct_track_points(self, track_points, ref_points):
        """runs the correction of the track_points using ref_points"""
        sid_delta = track_points['sid'][1:] - track_points['sid'][:-1]
        idx = np.where(sid_delta != 0)[0]
        for sid in idx:
            cur_pt = track_points[sid]
            next_pt = track_points[sid+1]
            dist = calculate_distance(cur_pt, next_pt)
            if dist > self.MIN_SEPARATION_CORRECTION_DIST_M:
                self.log.info(f'Correcting segment between points: {sid}-{sid+1}, distance: {dist} m')
                track_points = self.correct(track_points, ref_points, sid)

        return track_points

    def correct(self, track_pts, ref_pts, break_id):
        """fills the gap of broken track_pts sequence using ref_pts"""
        cur_pt = track_pts[break_id]
        next_pt = track_pts[break_id+1]

        cur_closest_ref_id = self.find_closest(cur_pt, ref_pts, 0)
        next_closest_ref_id = self.find_closest(next_pt, ref_pts, cur_closest_ref_id)

        dist_track_points = calculate_distance(cur_pt, next_pt)
        cur_ref2next_track = calculate_distance(ref_pts[cur_closest_ref_id], next_pt)
        next_ref2cur_track = calculate_distance(ref_pts[next_closest_ref_id], cur_pt)

        if dist_track_points < cur_ref2next_track:
            cur_closest_ref_id += 1

        if dist_track_points < next_ref2cur_track:
            next_closest_ref_id -= 1

        ref_sel_pts = ref_pts[cur_closest_ref_id:next_closest_ref_id+1]
        ref_sel_pts['timestamp'] = interpolate_timestamps(cur_pt['timestamp'],
                                                          next_pt['timestamp'],
                                                          len(ref_sel_pts))
        ref_sel_pts['sid'] = cur_pt['sid']
        ref_sel_pts['pt_type'] = 'corrected'
        new_track_pts = track_pts[:break_id+1]
        new_track_pts = np.concatenate((new_track_pts, ref_sel_pts))
        new_track_pts = np.concatenate((new_track_pts, track_pts[break_id+1:]))

        return new_track_pts

    @staticmethod
    def find_closest(track_pt, ref_pts, start_idx):
        """finds closest reference point to the track_pt"""
        idxs = range(start_idx, len(ref_pts))
        closest_dist = np.inf
        closest_id = None
        for idx in idxs:
            dist = calculate_distance(track_pt, ref_pts[idx])
            if dist < closest_dist:
                closest_dist = dist
                closest_id = idx
        return closest_id

    def _validate_transport_mode(self, transport_mode):
        if transport_mode in self.MODES:
            return transport_mode
        else:
            self.log.error(f"{transport_mode} is unknown mean of transport, known are: {self.MODES}")
            raise ValueError(f"{transport_mode} is unknown mean of transport")

    def _check_stop(self, time_delta, dist, median_time_delta):
        """detects stop in point samples"""
        time_threshold = median_time_delta * 4
        dist_threshold = self.STOP_DIST_SLOW_MAX_M
        if self.transport_mode != 'walk':
            dist_threshold = self.STOP_DIST_FAST_MAX_M

        return time_delta > time_threshold and dist < dist_threshold


if __name__ == "__main__":
    import argparse

    argp = argparse.ArgumentParser()
    argp.add_argument('--mode',
                      help='Mean of transport, bike, run or walk',
                      required=True)
    argp.add_argument('--gpx-file',
                      dest='gpx_file',
                      help='path to the file to be processed',
                      required=True)
    argp.add_argument('--ref-file',
                      dest='ref_file',
                      help='path to the reference file to be used for correction',
                      default=None)
    argp.add_argument('--index',
                      help='elasticsearch index to be used for data storage, if None, no indexing will happen',
                      default=None)
    argp.add_argument('--start',
                      help='Isoformat time of a trip start, set for untracked trips, None for tracked or planned trips',
                      default=None)
    argp.add_argument('--end',
                      help='Isoformat time of a trip end, set for untracked trips, None for tracked or planned trips',
                      default=None)

    params = argp.parse_args()

    tracker = GpxTripTracker(params.mode, params.gpx_file, params.ref_file,
                             start=params.start,
                             end=params.end,
                             index=params.index)
    tracker.run()
