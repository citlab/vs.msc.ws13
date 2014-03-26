package de.tu_berlin.citlab.cluster.gson;

public class DescribeInstancesGson {
	private ReservationsGson[] Reservations;

	public ReservationsGson[] getReservations() {
		return Reservations;
	}

	public void setReservations(ReservationsGson[] reservations) {
		this.Reservations = reservations;
	}
}
