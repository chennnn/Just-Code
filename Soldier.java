package com.hb;

public class Soldier {
	private Weapon weapon;

	public void setWeapon(Weapon weapon) {
		this.weapon = weapon;
	}

	public void kill() {
		weapon.kill();
	}
}
