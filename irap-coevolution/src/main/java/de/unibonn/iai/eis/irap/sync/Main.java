/**
 * 
 */
package de.unibonn.iai.eis.irap.sync;

import de.unibonn.iai.eis.irap.changeset.ChangesetManager;

/**
 * @author Kemele M. Endris
 *
 */
public class Main {
	
	public static void main(String[] args) {
		new ChangesetManager().begin();
	}

}
