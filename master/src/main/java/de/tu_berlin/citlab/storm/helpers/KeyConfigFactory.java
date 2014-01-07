package de.tu_berlin.citlab.storm.helpers;

import java.io.Serializable;

import de.tu_berlin.citlab.storm.window.IKeyConfig;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class KeyConfigFactory implements Serializable {
	
	private static IKeyConfig defaultKey = new IKeyConfig() {
		private Serializable defaultKey = 0;
		public Serializable getKeyOf(Tuple input) {
			return defaultKey;
		}
	};
	
	private static IKeyConfig bySource = new IKeyConfig() {
		public Serializable getKeyOf(Tuple input) {
			return input.getSourceComponent();
		}
	};
	
	public static IKeyConfig ByFields(final String... fields) {
		return ByFields(new Fields(fields));
	}
	
	public static IKeyConfig ByFields(final Fields keyFields) {
		return new IKeyConfig() {
			public Serializable getKeyOf(Tuple input) {
				return (Serializable) input.select(keyFields);
			}
		};
	}
	
	public static IKeyConfig BySource() {
		return bySource;
	}
	
	public static IKeyConfig DefaultKey() {
		return defaultKey;
	}

}
