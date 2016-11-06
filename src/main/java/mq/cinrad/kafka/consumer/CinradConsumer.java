package mq.cinrad.kafka.consumer;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.BlockingQueue;

import javax.sql.DataSource;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.referencing.FactoryException;
import org.postgis.jts.JtsGeometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.PrecisionModel;

import mq.radar.cinrad.Cinrad;
import mq.radar.cinrad.decoders.DecodeException;
import mq.radar.cinrad.decoders.cinrad.UnSupportProductException;

public class CinradConsumer implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(CinradConsumer.class);

	private BlockingQueue<ConsumerRecord<String, String>> recordQueue;

	private GeometryFactory geoFactory = new GeometryFactory(new PrecisionModel(100000), 4326);

	private Connection connection = null;

	final int batchSize = 20;

	private DataSource cinradDataSource;
	
	

	public CinradConsumer(BlockingQueue<ConsumerRecord<String, String>> recordQueue, DataSource dataSource) {
		this.recordQueue = recordQueue;
		this.cinradDataSource = dataSource;
		
	}

	@Override
	public void run() {

		while (true) {

			try {
				ConsumerRecord<String, String> record = recordQueue.take();
				if (record.key().endsWith(".bin") && !record.key().contains("Z_R_DWRN_SRSI")) {
					//processRecord(record);
					processRecord(record);
				}
			} catch (InterruptedException e) {

				logger.error(e.getMessage());
			}

		}

	}
	@SuppressWarnings("unused")
	private void processRecord2(ConsumerRecord<String, String> data) {
		System.out.println("hello world");
		System.out.println(cinradDataSource);
		
	}

	private void processRecord(ConsumerRecord<String, String> data) {

		logger.info("Process Record ,Topic:{} ,Key:{} ,Value:{}", data.topic(), data.key(), data.value());

		Cinrad cinrad = null;
		try {
			connection = this.cinradDataSource.getConnection();
			cinrad = new Cinrad(new URL(data.value()));

			processCinradSid(cinrad);

			int pcode = cinrad.getHeader().getProductCode();

			switch (pcode) {
			case 19:
			case 20:
			case 37:
			case 38:
				cinrad.setDecodeHint(Utils.getRDecodeHint());
				cinrad.decodeData();
				storeRadial(cinrad);
				break;
			case 26:
			case 27:
			case 41:
			case 46:
			case 57:
			case 78:
			case 79:
			case 80:
				cinrad.decodeData();
				storeRadial(cinrad);
				break;
			// case 53:
			// case 110:
			// storeRadial(cinrad);
			// break;
			// case 60:
			// store60(cinrad);
			// break;
			// case 58:
			// store58(cinrad);
			// break;
			// case 48:
			// store48(cinrad);
			// break;
			default:
				break;
			}

		} catch (NumberFormatException e) {
			logger.error("NumberFormatException:", e);
		} catch (MalformedURLException e) {
			logger.error("MalformedURLException:", e);
		} catch (DecodeException e) {
			logger.error("DecodeException:", e);
		} catch (UnSupportProductException e) {
			logger.error("UnSupportProductException:", e);
		} catch (IOException e) {
			logger.error("IOException:", e);
		} catch (FactoryException e) {
			logger.error("FactoryException:", e);
		} catch (Exception e) {
			logger.error("Exception:", e);

		}

		finally {
			if (null != cinrad) {
				cinrad.getHeader().close();
				cinrad = null;
			}
			if (null != connection) {
				try {
					connection.close();
				} catch (SQLException e) {
					logger.error(e.getMessage());
				}
				// connection = null;
			}

		}

	}

	private void processCinradSid(Cinrad cinrad) {
		// TODO Auto-generated method stub
		int sid = Utils.getSidByLonLat(new LonLat(cinrad.getHeader().getLon(), cinrad.getHeader().getLat()));
		cinrad.getHeader().setRadarStationID(sid);

	}

	private void storeRadial(Cinrad cinrad) {

		//if (null != cinrad.getDecoder().getFeatures() && cinrad.getDecoder().getFeatures().size() > 0) {

			Date date = DateUtils.truncate(cinrad.getHeader().getScanCalendar().getTime(), Calendar.MINUTE);

			java.sql.Timestamp sqlDate = new java.sql.Timestamp(date.getTime());

			long id = -1;

			// String sql =
			// "INSERT INTO radial_index(id, sid, pcode, elev, scale_time)
			// VALUES
			// (radial_index_id_seq.nextval , ?, ?, ?, ?)";

			String sql = "INSERT INTO radial_index( sid, pcode, elev, scale_time) VALUES ( ?, ?, ?, ?)";

			PreparedStatement psIndex = null;

			PreparedStatement psItem = null;

			PreparedStatement psUpdate = null;

			try {
				psIndex = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
				psIndex.setString(1, cinrad.getHeader().getRadarStationID().toString());
				psIndex.setInt(2, cinrad.getHeader().getProductCode());
				psIndex.setShort(3, cinrad.getHeader().getElevNumber());
				psIndex.setTimestamp(4, sqlDate);

				// ps.executeUpdate(sql);
				psIndex.execute();
				connection.commit();

				ResultSet rs = psIndex.getGeneratedKeys();

				if (rs.next()) {

					id = rs.getLong(1);
				}

				logger.info("radial_index_id:" + id);
				logger.info(sqlDate.toString());
				if (id != -1) {

					// String sql2 = "INSERT INTO radial_"
					// + cinrad.getHeader().getProductCode()
					// + "(gid, radial_id, color, the_geom)VALUES (radial_"
					// + cinrad.getHeader().getProductCode()
					// + "_gid_seq.nextval, ?, ?, ?)";

					String sql2 = "INSERT INTO radial_" + cinrad.getHeader().getProductCode()
							+ "(radial_id, color, the_geom)VALUES ( ?, ?, ?)";

					SimpleFeatureIterator iterator = cinrad.getDecoder().getFeatures().features();

					psItem = connection.prepareStatement(sql2);
					while (iterator.hasNext()) {
						SimpleFeature f = iterator.next();

						if (f.getDefaultGeometry() instanceof Polygon) {
							psItem.setLong(1, id);
							psItem.setInt(2, (Integer) f.getAttribute("colorIndex"));
							Polygon[] pa = { (Polygon) f.getDefaultGeometry() };
							// MultiPolygon multiPolygon=new MultiPolygon(pa,
							// geoFactory);
							psItem.setObject(3, new JtsGeometry(new MultiPolygon(pa, geoFactory)));
							psItem.addBatch();

						} else if (f.getDefaultGeometry() instanceof MultiPolygon) {

							psItem.setLong(1, id);
							psItem.setInt(2, (Integer) f.getAttribute("colorIndex"));
							psItem.setObject(3, new JtsGeometry((MultiPolygon) f.getDefaultGeometry()));
							psItem.addBatch();

						}

					}

					psItem.executeBatch();
					connection.commit();
					psItem.clearBatch();

					String sql3 = "UPDATE  radial_index SET finished = true WHERE id = ?";
					psUpdate = connection.prepareStatement(sql3);
					psUpdate.setLong(1, id);
					psUpdate.execute();
					connection.commit();

				}
			} catch (SQLException e1) {

				logger.error("SQLException:", e1);
			} finally {

				// cinrad.getDecoder().getFeatures().clear();
				cinrad = null;
				try {
					if (psIndex != null) {
						psIndex.close();
						psIndex = null;
					}
					if (psItem != null) {
						psItem.close();
						psItem = null;
					}
					if (psUpdate != null) {
						psUpdate.close();
						psUpdate = null;
					}
				} catch (SQLException e) {
					logger.error("SQLException:", e);
				}

			}
	//	}

	}

	private void store48(Cinrad cinrad) {

		Date date = DateUtils.truncate(cinrad.getHeader().getScanCalendar().getTime(), Calendar.MINUTE);
		java.sql.Timestamp sqlDate = new java.sql.Timestamp(date.getTime());

		// String sql =
		// "INSERT INTO radar_48(gid, scale_time, sid, lat, lon, alt, dir, spd,
		// rms, the_geom) VALUES (radar_48_gid_seq.nextval, ?, ?, ?, ?, ?, ?, ?,
		// ?, ?)";

		String sql = "INSERT INTO radar_48(scale_time, sid, lat, lon, alt, dir, spd, rms, the_geom) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

		PreparedStatement ps = null;

		try {
			ps = connection.prepareStatement(sql);

			SimpleFeatureIterator iterator = cinrad.getDecoder().getFeatures().features();

			int k = 1;

			while (iterator.hasNext()) {
				SimpleFeature f = iterator.next();
				ps.setTimestamp(1, sqlDate);
				ps.setInt(2, cinrad.getHeader().getRadarStationID());
				ps.setDouble(3, (Double) f.getAttribute("lat"));
				ps.setDouble(4, (Double) f.getAttribute("lon"));
				ps.setDouble(5, (Double) f.getAttribute("alt"));
				ps.setInt(6, (Integer) f.getAttribute("dir"));
				ps.setDouble(7, Math.floor((Double) f.getAttribute("spd") * 10.0) / 10.0);
				ps.setInt(8, (Integer) f.getAttribute("rms"));
				ps.setObject(9, new JtsGeometry((Point) f.getDefaultGeometry()));

				ps.addBatch();

				if (k % batchSize == 0) {
					ps.executeBatch();
					connection.commit();
					ps.clearBatch();
				}
				k++;

			}

			ps.executeBatch();
			connection.commit();
			ps.clearBatch();

		} catch (SQLException e) {
			logger.error("SQLException:", e);
		} finally {

			// cinrad.getDecoder().getFeatures().clear();
			cinrad = null;

			try {
				if (ps != null) {
					ps.close();
					ps = null;
				}

			} catch (SQLException e) {
				logger.error("SQLException:", e);
			}

		}

	}

	private void store58(Cinrad cinrad) {

		Date date = DateUtils.truncate(cinrad.getHeader().getScanCalendar().getTime(), Calendar.MINUTE);
		java.sql.Timestamp sqlDate = new java.sql.Timestamp(date.getTime());

		String sql = "INSERT INTO radar_58( scale_time, sid, lat, lon, id, ran, azim, movedeg, movekts, the_geom) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		// String sql =
		// "INSERT INTO radar_58(gid, scale_time, sid, lat, lon, id, ran, azim,
		// movedeg, movekts, the_geom) VALUES (radar_58_gid_seq.nextval, ?, ?,
		// ?, ?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement ps = null;

		try {
			ps = connection.prepareStatement(sql);

			SimpleFeatureIterator iterator = cinrad.getDecoder().getLineFeatures().features();

			int k = 1;

			while (iterator.hasNext()) {
				SimpleFeature f = iterator.next();

				ps.setTimestamp(1, sqlDate);
				ps.setInt(2, cinrad.getHeader().getRadarStationID());
				ps.setDouble(3, (Double) f.getAttribute("lat"));
				ps.setDouble(4, (Double) f.getAttribute("lon"));
				ps.setString(5, (String) f.getAttribute("id"));
				ps.setDouble(6, (Double) f.getAttribute("range"));
				ps.setDouble(7, (Double) f.getAttribute("azim"));
				String deg = f.getAttribute("movedeg").toString().trim();
				String kts = f.getAttribute("movekts").toString().trim();
				if (deg.equalsIgnoreCase("NEW") || kts.equalsIgnoreCase("NEW")) {
					ps.setDouble(8, -999.9);
					ps.setDouble(9, -999.9);
				} else {
					ps.setDouble(8, Double.valueOf(deg));
					ps.setDouble(9, Double.valueOf(kts));
				}
				ps.setObject(10, new JtsGeometry((LineString) f.getDefaultGeometry()));

				ps.addBatch();

				if (k % batchSize == 0) {
					ps.executeBatch();
					connection.commit();
					ps.clearBatch();
				}
				k++;

			}

			ps.executeBatch();
			connection.commit();
			ps.clearBatch();

		} catch (SQLException e) {

			logger.error("SQLException:", e);
		} finally {
			// cinrad.getDecoder().getFeatures().clear();
			// cinrad.getDecoder().getLineFeatures().clear();
			cinrad = null;

			try {
				if (ps != null) {
					ps.close();
					ps = null;
				}

			} catch (SQLException e) {
				logger.error("SQLException:", e);
			}

		}

	}

	private void store60(Cinrad cinrad) {
		Date date = DateUtils.truncate(cinrad.getHeader().getScanCalendar().getTime(), Calendar.MINUTE);
		java.sql.Timestamp sqlDate = new java.sql.Timestamp(date.getTime());

		String sql = "INSERT INTO radar_60( scale_time, sid, lat, lon, id, type, ran, azim, basehgt, tophgt, height, radius, azdia, shear, the_geom) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		// String sql =
		// "INSERT INTO radar_60(gid, scale_time, sid, lat, lon, id, type, ran,
		// azim, basehgt, tophgt, height, radius, azdia, shear, the_geom) VALUES
		// (radar_60_gid_seq.nextval, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		// ?)";
		PreparedStatement ps = null;

		try {
			ps = connection.prepareStatement(sql);

			SimpleFeatureIterator iterator = cinrad.getDecoder().getFeatures().features();

			int k = 1;
			while (iterator.hasNext()) {
				SimpleFeature f = iterator.next();

				ps.setTimestamp(1, sqlDate);
				ps.setInt(2, cinrad.getHeader().getRadarStationID());
				ps.setDouble(3, (Double) f.getAttribute("lat"));
				ps.setDouble(4, (Double) f.getAttribute("lon"));
				ps.setString(5, (String) f.getAttribute("id"));
				ps.setString(6, (String) f.getAttribute("type"));
				ps.setDouble(7, (Double) f.getAttribute("range"));
				ps.setDouble(8, (Double) f.getAttribute("azim"));
				ps.setString(9, (String) f.getAttribute("basehgt"));
				ps.setString(10, (String) f.getAttribute("tophgt"));
				ps.setString(11, (String) f.getAttribute("height"));
				ps.setString(12, (String) f.getAttribute("radius"));
				ps.setString(13, (String) f.getAttribute("azdia"));
				ps.setString(14, (String) f.getAttribute("shear"));
				ps.setObject(15, new JtsGeometry((Point) f.getDefaultGeometry()));

				ps.addBatch();

				if (k % batchSize == 0) {
					ps.executeBatch();
					connection.commit();
					ps.clearBatch();
				}
				k++;

			}

			ps.executeBatch();
			connection.commit();
			ps.clearBatch();

		} catch (SQLException e) {
			logger.error("SQLException:", e);
		} finally {
			// cinrad.getDecoder().getFeatures().clear();
			// cinrad.getDecoder().getLineFeatures().clear();
			cinrad = null;

			try {
				if (ps != null) {
					ps.close();
					ps = null;
				}

			} catch (SQLException e) {
				logger.error("SQLException:", e);
			}

		}
	}

}
