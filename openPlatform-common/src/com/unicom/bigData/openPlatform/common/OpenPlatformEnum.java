package com.unicom.bigData.openPlatform.common;

public interface OpenPlatformEnum {

	String getName();

	public enum SubserviceSubTypes implements OpenPlatformEnum {

		power_on_pos("1"), area_entrace("2"), area_leaving("3"), interval_notification("4"), context_change(
				"5"), area_density("6");

		private String typeId;

		private SubserviceSubTypes(String typeId) {
			this.typeId = typeId;
		}

		public String getTypeId() {
			return this.typeId;
		}

		@Override
		public String getName() {
			return this.name();
		}
	}

	public enum RedisGlobalKeys implements OpenPlatformEnum {
		pos_, pos_his_, pos_users_, rel_pos_area_, area_density_, user_context_, user_context_his_, secretTable, subscribe_service_list, new_subscribe_piriod_jobs, delete_subscribe_piriod_jobs, query_Interfaces_status;

		@Override
		public String getName() {
			return this.name();
		}
	}

	public enum RedisPositonKeys implements OpenPlatformEnum {
		lac, cellId, x, y, province, city, country;

		@Override
		public String getName() {
			return this.name();
		}
	}

	public enum RedisUserContextKeys implements OpenPlatformEnum {
		terminalBrand, terminalType, appCategory, appSubCategory, URL;

		@Override
		public String getName() {
			return this.name();
		}
	}

	public enum Tables implements OpenPlatformEnum {
		op_sub_service_info(1), op_area_bs_relation(2), op_user_position_info(3), op_area_density(4), op_user_base_info(
				null), op_dict_sub_service_type(null), D_ST_DEVICE(null), TG_RG_WORKING(null), TG_RG_RESIDENCE(
						null), B_CUST(null), ST_U2U(null), M_ST_GROUP_CALL(null), M_ST_CALL(null), D_ST_CALL(
								null), D_ST_IA(null), M_ST_IA(null), TG_IA_APP(null), BASE_STATION_INFO(null);

		private Integer workId;

		private Tables(Integer workId) {
			this.workId = workId;
		}

		public Integer getWorkId() {
			return this.workId;
		}

		@Override
		public String getName() {
			return this.name();
		}
	}

	public enum State implements OpenPlatformEnum {

		C200("成功"), C400("失败"), C401("Unauthorized"), C500("服务器异常");

		private String message;

		private State(String message) {
			this.message = message;
		}

		public String message() {
			return this.message;
		}

		@Override
		public String getName() {
			return this.name();
		}
	}
}
