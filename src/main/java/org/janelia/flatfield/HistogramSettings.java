package org.janelia.flatfield;

import java.io.Serializable;

public class HistogramSettings implements Serializable
{
	private static final long serialVersionUID = 3131613367735578708L;

	public final Double histMinValue, histMaxValue;
	public final int bins;

	public HistogramSettings( final Double histMinValue, final Double histMaxValue, final int bins )
	{
		this.histMinValue = histMinValue;
		this.histMaxValue = histMaxValue;
		this.bins = bins;
	}

	public boolean isValid()
	{
		return histMinValue != null && histMaxValue != null;
	}
}
