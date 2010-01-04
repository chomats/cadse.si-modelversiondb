/**
 * 
 */
package fr.imag.adele.teamwork.db.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;

import fr.imag.adele.teamwork.db.DBIteratorID;
import fr.imag.adele.teamwork.db.ID3;
import fr.imag.adele.teamwork.db.LinkInfo;
import fr.imag.adele.teamwork.db.LinkInfoPlus;
import fr.imag.adele.teamwork.db.ModelVersionDBService2;

abstract class DBIteratorIDImpl<T> extends DBIteratorID<T> {
	
	/**
	 * The result set must contains this information in this order.
	 * <ul>
	 * <li>cadseTypeId</li>
		<li>typeId</li>
			<li>objectId</li>
		
	 * </ul>
	 * @author chomats
	 *
	 */
	public static class IterID3 extends DBIteratorIDImpl<ID3> {

		public IterID3(ModelVersionDBImpl2 db, ResultSet rs) {
			super(db, rs);
			_id = new ID3(ModelVersionDBService2.NULL_ID, ModelVersionDBService2.NULL_ID, ModelVersionDBService2.NULL_ID);
		}

		@Override
		protected void fill() throws SQLException {
			_id.cadseTypeId = _rs.getInt(1);
			_id.typeId = _rs.getInt(2);
			_id.objectId = _rs.getInt(3);
		}
		
	}
	
	/**
	 * The result set must contains this information in this order.
	 * <ul>
	 * <li>linkId</li>
		<li>linkTypeId</li>
			<li>sourceId</li>
			<li>destId</li>
		
	 * </ul>
	 * @author chomats
	 *
	 */
	public static class IterLinkInfo extends DBIteratorIDImpl<LinkInfo> {

		public IterLinkInfo(ModelVersionDBImpl2 db, ResultSet rs) {
			super(db, rs);
			_id = new LinkInfo(ModelVersionDBService2.NULL_ID, ModelVersionDBService2.NULL_ID, 
					ModelVersionDBService2.NULL_ID, ModelVersionDBService2.NULL_ID);
		}

		@Override
		protected void fill() throws SQLException {
			_id.linkId = _rs.getInt(1);
			_id.linkTypeId = _rs.getInt(2);
			_id.sourceId = _rs.getInt(3);
			_id.destId = _rs.getInt(3);
		}
		
	}
	
	
	/**
	 * The result set must contains this information in this order.
	 * <ul>
	 * <li>linkId</li>
		<li>linkTypeId : <ul>
	 * <li>cadseTypeId</li>
		<li>typeId</li>
			<li>objectId</li>
		
	 * </ul></li>
			<li>sourceId: <ul>
	 * <li>cadseTypeId</li>
		<li>typeId</li>
			<li>objectId</li>
		
	 * </ul></li>
			<li>destId: <ul>
	 * <li>cadseTypeId</li>
		<li>typeId</li>
			<li>objectId</li>
		
	 * </ul></li>
		
	 * </ul>
	 * @author chomats
	 *
	 */
	public static class IterLinkInfoPlus extends DBIteratorIDImpl<LinkInfoPlus> {

		public IterLinkInfoPlus(ModelVersionDBImpl2 db, ResultSet rs) {
			super(db, rs);
			_id = new LinkInfoPlus(ModelVersionDBService2.NULL_ID, 
					new ID3(ModelVersionDBService2.NULL_ID, ModelVersionDBService2.NULL_ID, ModelVersionDBService2.NULL_ID), 
					new ID3(ModelVersionDBService2.NULL_ID, ModelVersionDBService2.NULL_ID, ModelVersionDBService2.NULL_ID),
					new ID3(ModelVersionDBService2.NULL_ID, ModelVersionDBService2.NULL_ID, ModelVersionDBService2.NULL_ID));
		}

		@Override
		protected void fill() throws SQLException {
			_id.linkId = _rs.getInt(1);
			
			_id.linkTypeId.cadseTypeId = _rs.getInt(2);
			_id.linkTypeId.typeId = _rs.getInt(3);
			_id.linkTypeId.objectId = _rs.getInt(4);
			
			_id.sourceId.cadseTypeId = _rs.getInt(5);
			_id.sourceId.typeId = _rs.getInt(6);
			_id.sourceId.objectId = _rs.getInt(7);
			
			_id.destId.cadseTypeId = _rs.getInt(8);
			_id.destId.typeId = _rs.getInt(9);
			_id.destId.objectId = _rs.getInt(10);
		}
		
	}
	
	ResultSet _rs;
	ModelVersionDBImpl2 _db;

	public DBIteratorIDImpl(ModelVersionDBImpl2 db, ResultSet rs) {
		super();
		_db = db;
		_rs = rs;
	}

    @Override
	public boolean hasNext() {
		try {
			return _rs.next();
		} catch (SQLException e) {
			close();
			_db.m_logger.log(Level.SEVERE, "Iterator ",e);
			return false;
		}
	}

    @Override
	public void close() {
		_db.close(_rs);
	}

    @Override
	public T next() {
		try {
			fill();
			return _id;
		} catch (SQLException e) {
			close();
			_db.m_logger.log(Level.SEVERE, "Iterator ",e);
			return null;
		}
	}

    abstract protected void fill() throws SQLException;

	@Override
	protected void finalize() throws Throwable {
		close();
	}
}